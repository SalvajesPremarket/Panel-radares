import threading
import time
from typing import Any, Dict, Iterable, List, Set

from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream


class _LiveCache:
    def __init__(self):
        self._lock = threading.RLock()
        self._trades: Dict[str, Dict[str, Any]] = {}
        self._quotes: Dict[str, Dict[str, Any]] = {}

    def trade(self, symbol: str):
        with self._lock:
            value = self._trades.get(str(symbol or "").upper())
            return dict(value) if value else None

    def quote(self, symbol: str):
        with self._lock:
            value = self._quotes.get(str(symbol or "").upper())
            return dict(value) if value else None

    def set_trade(self, symbol: str, price: Any, timestamp: Any = None):
        key = str(symbol or "").upper()
        if key:
            with self._lock:
                self._trades[key] = {"price": price, "timestamp": timestamp}

    def set_quote(self, symbol: str, bid: Any, ask: Any, timestamp: Any = None):
        key = str(symbol or "").upper()
        if key:
            with self._lock:
                self._quotes[key] = {"bid": bid, "ask": ask, "timestamp": timestamp}


class AlpacaMarketStream:
    def __init__(self, api_key: str, secret_key: str, feed: str = "iex", max_symbols: int = 30):
        self.api_key = api_key
        self.secret_key = secret_key
        self.feed = str(feed or "iex").strip().lower()
        self.max_symbols = max(1, int(max_symbols or 30))
        self._lock = threading.RLock()
        self._stream = None
        self._thread = None
        self._symbols: Set[str] = set()
        self._quotes = 0
        self._trades = 0
        self._symbols_seen: Set[str] = set()
        self._errors = 0
        self._last_event_ts = 0.0
        self._connected = False
        self._running = False
        self._last_error = ""
        self._last_subscription_change = 0.0
        self._subscription_min_interval = 20.0
        self.cache = _LiveCache()
        self._trade_consumers = []
        self._quote_consumers = []

    # API estable del stream compartido. No abrir otro websocket para consumidores.
    def add_consumer(self, trade_callback=None, quote_callback=None):
        """Registra consumidores adicionales sin abrir otro websocket."""
        with self._lock:
            if trade_callback is not None and trade_callback not in self._trade_consumers:
                self._trade_consumers.append(trade_callback)
            if quote_callback is not None and quote_callback not in self._quote_consumers:
                self._quote_consumers.append(quote_callback)

    def remove_consumer(self, trade_callback=None, quote_callback=None):
        with self._lock:
            if trade_callback in self._trade_consumers:
                self._trade_consumers.remove(trade_callback)
            if quote_callback in self._quote_consumers:
                self._quote_consumers.remove(quote_callback)

    def _feed_enum(self):
        return DataFeed.SIP if self.feed == "sip" else DataFeed.IEX

    @staticmethod
    def _symbol(data: Any) -> str:
        value = getattr(data, "symbol", None)
        if value is None and isinstance(data, dict):
            value = data.get("S") or data.get("symbol")
        return str(value or "").strip().upper()

    async def _on_quote(self, data: Any):
        with self._lock:
            self._quotes += 1
            self._last_event_ts = time.time()
            self._connected = True
            symbol = self._symbol(data)
            if symbol:
                self._symbols_seen.add(symbol)
                self.cache.set_quote(symbol, getattr(data, "bid_price", None), getattr(data, "ask_price", None), getattr(data, "timestamp", None))
            consumidores = list(self._quote_consumers)
        for callback in consumidores:
            try:
                callback(data)
            except Exception:
                pass

    async def _on_trade(self, data: Any):
        with self._lock:
            self._trades += 1
            self._last_event_ts = time.time()
            self._connected = True
            symbol = self._symbol(data)
            if symbol:
                self._symbols_seen.add(symbol)
                self.cache.set_trade(symbol, getattr(data, "price", None), getattr(data, "timestamp", None))
            consumidores = list(self._trade_consumers)
        for callback in consumidores:
            try:
                callback(data)
            except Exception:
                pass

    def _create_stream_locked(self):
        if self._stream is None:
            self._stream = StockDataStream(self.api_key, self.secret_key, feed=self._feed_enum())

    def _run_stream(self):
        with self._lock:
            stream = self._stream
            self._running = True
        try:
            if stream is not None:
                stream.run()
        except Exception as exc:
            with self._lock:
                self._errors += 1
                self._last_error = str(exc)
                self._connected = False
        finally:
            with self._lock:
                self._running = False
                self._connected = False
                # Si la conexión murió por error, no conservamos un objeto
                # StockDataStream muerto: el siguiente start() podrá crear uno nuevo.
                if self._stream is stream:
                    self._stream = None
                    self._symbols.clear()

    def start(self, tickers: Iterable[str]):
        symbols: List[str] = []
        seen = set()
        for raw in tickers or []:
            symbol = str(raw or "").strip().upper()
            if symbol and symbol not in seen:
                seen.add(symbol)
                symbols.append(symbol)
            if len(symbols) >= self.max_symbols:
                break
        if not symbols:
            return
        with self._lock:
            nuevos = set(symbols)
            # No renegociamos la suscripción en cada ciclo de 10 s. El radar
            # puede cambiar de candidatos muy rápido y eso provoca tráfico
            # innecesario de subscribe/unsubscribe en Alpaca. Conservamos la
            # última lista durante unos segundos y evitamos churn del websocket.
            if self._stream is not None and nuevos != self._symbols:
                if time.monotonic() - self._last_subscription_change < self._subscription_min_interval:
                    return
            if self._stream is None:
                self._create_stream_locked()
                # En Alpaca Basic, una suscripción de trades también añade
                # corrections/cancelErrors. Con 10 símbolos eso ocupa el límite
                # de 30 canales de símbolos; no agregamos quotes encima.
                self._stream.subscribe_trades(self._on_trade, *symbols)
                self._symbols.update(symbols)
            else:
                quitar = self._symbols - nuevos
                agregar = nuevos - self._symbols

                # Alpaca procesa unsubscribe/subscribe de forma asíncrona.
                # Primero reducimos la suscripción; el siguiente ciclo agregará
                # los símbolos faltantes cuando el servidor ya haya procesado el
                # unsubscribe. Así nunca solicitamos más de max_symbols.
                if quitar:
                    self._stream.unsubscribe_trades(*sorted(quitar))
                    self._symbols.difference_update(quitar)
                    self._last_subscription_change = time.monotonic()
                    return

                if agregar:
                    capacidad = max(0, self.max_symbols - len(self._symbols))
                    agregar = sorted(agregar)[:capacidad]
                    if agregar:
                        self._stream.subscribe_trades(self._on_trade, *agregar)
                        self._symbols.update(agregar)

            self._last_subscription_change = time.monotonic()
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run_stream, name="alpaca-market-stream", daemon=True)
                self._thread.start()

    def stop(self):
        with self._lock:
            stream = self._stream
            self._stream = None
            self._thread = None
            self._symbols.clear()
            self._running = False
            self._connected = False
        if stream is not None:
            try:
                stream.stop()
            except Exception:
                pass

    def health_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "connected": bool(self._connected),
                "feed": self.feed,
                "quotes": int(self._quotes),
                "trades": int(self._trades),
                "symbols_seen": len(self._symbols_seen),
                "errors": int(self._errors),
                "last_event_ts": float(self._last_event_ts),
                "last_error": self._last_error,
                "subscribed_symbols": len(self._symbols),
                "running": bool(self._running),
            }
