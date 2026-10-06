"""Adaptador aislado de Alpaca WebSocket para datos de mercado."""

from __future__ import annotations

import threading
from typing import Callable, Iterable

from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream

from .data_health import DataHealth
from .market_cache import MarketCache
from .bar_builder import LiveBarBuilder


class AlpacaMarketStream:
    """Una conexión WebSocket por proceso, reutilizable por todo el scanner.

    El feed se selecciona explícitamente: iex por defecto; sip puede habilitarse
    mediante configuración cuando la cuenta tenga acceso a SIP.
    """

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        feed: str = "iex",
        cache: MarketCache | None = None,
        bars: LiveBarBuilder | None = None,
        health: DataHealth | None = None,
        on_bar: Callable | None = None,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.secret_key = str(secret_key or "").strip()
        self.feed_name = str(feed or "iex").strip().lower()
        self.cache = cache or MarketCache()
        self.bars = bars or LiveBarBuilder()
        self.health = health or DataHealth()
        self.on_bar = on_bar
        self._stream: StockDataStream | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._symbols: set[str] = set()

    def _feed(self):
        if self.feed_name == "sip":
            return DataFeed.SIP
        if self.feed_name == "delayed_sip":
            return DataFeed.DELAYED_SIP
        return DataFeed.IEX

    async def _quote(self, data) -> None:
        symbol = getattr(data, "symbol", "")
        self.cache.update_quote(
            symbol,
            getattr(data, "bid_price", None),
            getattr(data, "ask_price", None),
            getattr(data, "timestamp", None),
        )
        self.health.mark_quote(symbol)

    async def _trade(self, data) -> None:
        symbol = getattr(data, "symbol", "")
        price = getattr(data, "price", None)
        size = getattr(data, "size", 0)
        ts = getattr(data, "timestamp", None)
        self.cache.update_trade(symbol, price, size, ts)
        self.health.mark_trade(symbol)
        for bar in self.bars.on_trade(symbol, price, size, ts):
            if self.on_bar is not None:
                self.on_bar(bar)

    def _run(self) -> None:
        try:
            self.health.start(self.feed_name)
            self._stream = StockDataStream(
                self.api_key,
                self.secret_key,
                feed=self._feed(),
            )
            if self._symbols:
                symbols = sorted(self._symbols)
                self._stream.subscribe_quotes(self._quote, *symbols)
                self._stream.subscribe_trades(self._trade, *symbols)
            self.health.mark_connected()
            self._stream.run()
        except Exception as exc:
            self.health.mark_error(exc)
            self.health.connected = False

    def start(self, symbols: Iterable[str]) -> None:
        requested = {
            str(symbol).strip().upper()
            for symbol in (symbols or [])
            if str(symbol).strip()
        }
        self._symbols = requested
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="TradeScanner-AlpacaWS",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        stream = self._stream
        if stream is not None:
            try:
                stream.stop()
            except Exception:
                pass
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=3)
        self._thread = None
        self._stream = None

    def health_snapshot(self) -> dict:
        return self.health.snapshot()
