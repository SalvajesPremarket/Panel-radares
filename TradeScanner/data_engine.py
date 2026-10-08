import os
import os
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
        self._stop_requested = False
        self._last_subscription_change = 0.0
        self._subscription_min_interval = 20.0
        self._last_subscription_request = 0.0
        self._last_event_kind = ""
        self._last_event_symbol = ""
        self._last_subscription_request = 0.0
        self._last_event_kind = ""
        self._last_event_symbol = ""
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
            self._last_event_kind = "quote"
            self._last_event_kind = "quote"
            symbol = self._symbol(data)
            if symbol:
                self._symbols_seen.add(symbol)
                self._last_event_symbol = symbol
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
            self._last_event_kind = "trade"
            self._last_event_kind = "trade"
            symbol = self._symbol(data)
            if symbol:
                self._symbols_seen.add(symbol)
                self._last_event_symbol = symbol
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
            self._running = True
            self._stop_requested = False
            self._last_error = "DEBUG: _run_stream entró"

        while True:
            with self._lock:
                if self._stop_requested:
                    break
                stream = self._stream
                if stream is None:
                    self._create_stream_locked()
                    stream = self._stream
                    if stream is not None:
                        self._last_error = f"DEBUG: stream creado/reconectado ({len(self._symbols)} símbolos)"
                        if self._symbols:
                            symbols = sorted(self._symbols)
                            stream.subscribe_trades(self._on_trade, *symbols)
                            stream.subscribe_quotes(self._on_quote, *symbols)
                            self._last_subscription_request = time.time()

            try:
                if stream is None:
                    time.sleep(1.0)
                    continue
                stream.run()
                with self._lock:
                    if self._stop_requested:
                        break
                    self._errors += 1
                    self._last_error = "StockDataStream.run() terminó sin excepción; reconectando"
                    self._connected = False
                    if self._stream is stream:
                        self._stream = None
                time.sleep(1.0)
            except Exception as exc:
                with self._lock:
                    if self._stop_requested:
                        break
                    self._errors += 1
                    self._last_error = str(exc)
                    self._connected = False
                    if self._stream is stream:
                        self._stream = None
                time.sleep(1.0)

        with self._lock:
            self._running = False
            self._connected = False
            self._stream = None

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
            self._stop_requested = False
            self._last_error = f"DEBUG: start() recibido ({len(symbols)} símbolos)"
            nuevos = set(symbols)
            # No renegociamos la suscripción en cada ciclo de 10 s. El radar
            # puede cambiar de candidatos muy rápido y eso provoca tráfico
            # innecesario de subscribe/unsubscribe en Alpaca. Conservamos la
            # última lista durante unos segundos y evitamos churn del websocket.
            if self._stream is not None and nuevos != self._symbols:
                if time.monotonic() - self._last_subscription_change < self._subscription_min_interval:
                    return
            if self._stream is None:
                # El cliente de Alpaca se crea y suscribe dentro del hilo que
                # ejecuta el event loop. Así todo el ciclo de vida del websocket
                # queda ligado al mismo hilo/event loop.
                self._symbols.update(symbols)
            else:
                quitar = self._symbols - nuevos
                agregar = nuevos - self._symbols

                # Alpaca procesa unsubscribe/subscribe de forma asíncrona.
                # Primero reducimos la suscripción; el siguiente ciclo agregará
                # los símbolos faltantes cuando el servidor ya haya procesado el
                # unsubscribe. Así nunca solicitamos más de max_symbols.
                if quitar:
                    symbols_quitar = sorted(quitar)
                    self._stream.unsubscribe_trades(*symbols_quitar)
                    self._stream.unsubscribe_quotes(*symbols_quitar)
                    self._symbols.difference_update(quitar)
                    self._last_subscription_request = time.time()
                    self._last_subscription_change = time.monotonic()
                    return

                if agregar:
                    capacidad = max(0, self.max_symbols - len(self._symbols))
                    agregar = sorted(agregar)[:capacidad]
                    if agregar:
                        self._stream.subscribe_trades(self._on_trade, *agregar)
                        self._stream.subscribe_quotes(self._on_quote, *agregar)
                        self._symbols.update(agregar)
                        self._last_subscription_request = time.time()

            self._last_subscription_change = time.monotonic()
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run_stream, name="alpaca-market-stream", daemon=True)
                self._last_error = "DEBUG: hilo del stream iniciado"
                self._thread.start()

    def stop(self):
        with self._lock:
            self._stop_requested = True
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
                "subscribed_symbols": sorted(self._symbols),
                "last_event_kind": self._last_event_kind,
                "last_event_symbol": self._last_event_symbol,
                "last_event_age_sec": (time.time() - self._last_event_ts) if self._last_event_ts else None,
                "last_subscription_request_ts": float(self._last_subscription_request),
                "subscribed_symbols": sorted(self._symbols),
                "last_event_kind": self._last_event_kind,
                "last_event_symbol": self._last_event_symbol,
                "last_event_age_sec": (time.time() - self._last_event_ts) if self._last_event_ts else None,
                "last_subscription_request_ts": float(self._last_subscription_request),
            }
