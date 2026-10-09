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
        max_symbols: int = 10,
        cache: MarketCache | None = None,
        bars: LiveBarBuilder | None = None,
        health: DataHealth | None = None,
        on_bar: Callable | None = None,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.secret_key = str(secret_key or "").strip()
        self.feed_name = str(feed or "iex").strip().lower()
        self.max_symbols = max(1, min(10, int(max_symbols or 10)))
        self.cache = cache or MarketCache()
        self.bars = bars or LiveBarBuilder()
        self.health = health or DataHealth()
        self.on_bar = on_bar
        self._stream: StockDataStream | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._symbols: set[str] = set()
        self._symbols_lock = threading.RLock()

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
                data_timeout=60,
            )
            with self._symbols_lock:
                symbols = sorted(self._symbols)
            if symbols:
                self._stream.subscribe_quotes(self._quote, *symbols)
                self._stream.subscribe_trades(self._trade, *symbols)
            # La conexión real se confirma al recibir el primer evento.
            self._stream.run()
        except Exception as exc:
            self.health.mark_error(exc)
            with self.health._lock:
                self.health.connected = False

    def start(self, symbols: Iterable[str]) -> None:
        requested = {
            str(symbol).strip().upper()
            for symbol in (symbols or [])
            if str(symbol).strip()
        }
        requested = set(sorted(requested)[:self.max_symbols])
        if not requested:
            # Do not open an authenticated websocket when the scanner has no
            # candidates. If already running, release the previous subscriptions.
            if self._thread and self._thread.is_alive():
                self._update_running_subscriptions(set())
            else:
                with self._symbols_lock:
                    self._symbols.clear()
            return
        if self._thread and self._thread.is_alive():
            self._update_running_subscriptions(requested)
            return
        with self._symbols_lock:
            self._symbols = requested
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="TradeScanner-AlpacaWS",
            daemon=True,
        )
        self._thread.start()


    def _update_running_subscriptions(self, requested: set[str]) -> None:
        with self._symbols_lock:
            old = set(self._symbols)
            self._symbols = set(requested)
            stream = self._stream
        # The websocket may still be connecting. Save the desired set now;
        # _run() will read it before subscribing, so updates are not lost.
        if stream is None:
            return
        add = requested - old
        remove = old - requested
        try:
            if add:
                symbols = sorted(add)
                stream.subscribe_quotes(self._quote, *symbols)
                stream.subscribe_trades(self._trade, *symbols)
            if remove:
                symbols = sorted(remove)
                stream.unsubscribe_quotes(*symbols)
                stream.unsubscribe_trades(*symbols)
        except Exception as exc:
            self.health.mark_error(exc)

    def update_symbols(self, symbols: Iterable[str]) -> None:
        requested = {
            str(symbol).strip().upper()
            for symbol in (symbols or [])
            if str(symbol).strip()
        }
        requested = set(sorted(requested)[:self.max_symbols])
        if self._thread and self._thread.is_alive():
            self._update_running_subscriptions(requested)
        else:
            with self._symbols_lock:
                self._symbols = requested

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
