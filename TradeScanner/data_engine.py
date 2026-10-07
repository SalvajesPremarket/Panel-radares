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
        self.cache = _LiveCache()

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

    async def _on_trade(self, data: Any):
        with self._lock:
            self._trades += 1
            self._last_event_ts = time.time()
            self._connected = True
            symbol = self._symbol(data)
            if symbol:
                self._symbols_seen.add(symbol)
                self.cache.set_trade(symbol, getattr(data, "price", None), getattr(data, "timestamp", None))

    def _create_stream_locked(self):
        if self._stream is None:
            self._stream = StockDataStream(self.api_key, self.secret_key, feed=self._feed_enum(), data_timeout=90)

    def _run_stream(self):
        try:
            with self._lock:
                self._running = True
            self._stream.run()
        except Exception as exc:
            with self._lock:
                self._errors += 1
                self._last_error = str(exc)
                self._connected = False
        finally:
            with self._lock:
                self._running = False
                self._connected = False

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
            if self._stream is None:
                self._create_stream_locked()
                self._stream.subscribe_quotes(self._on_quote, *symbols)
                self._stream.subscribe_trades(self._on_trade, *symbols)
            else:
                quitar = self._symbols - nuevos
                agregar = nuevos - self._symbols
                if quitar:
                    self._stream.unsubscribe_quotes(*sorted(quitar))
                    self._stream.unsubscribe_trades(*sorted(quitar))
                if agregar:
                    self._stream.subscribe_quotes(self._on_quote, *sorted(agregar))
                    self._stream.subscribe_trades(self._on_trade, *sorted(agregar))
            self._symbols = nuevos
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
