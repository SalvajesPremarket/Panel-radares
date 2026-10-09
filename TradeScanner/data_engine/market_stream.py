"""Adaptador aislado de Alpaca WebSocket para datos de mercado."""

from __future__ import annotations

import inspect
import threading
import time
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
        max_symbols: int = 7,
        cache: MarketCache | None = None,
        bars: LiveBarBuilder | None = None,
        health: DataHealth | None = None,
        on_bar: Callable | None = None,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.secret_key = str(secret_key or "").strip()
        self.feed_name = str(feed or "iex").strip().lower()
        self.max_symbols = max(1, min(7, int(max_symbols or 7)))
        self.cache = cache or MarketCache()
        self.bars = bars or LiveBarBuilder()
        self.health = health or DataHealth()
        self.on_bar = on_bar
        self._stream: StockDataStream | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._symbols: set[str] = set()
        self._symbols_lock = threading.RLock()
        self._subscription_lock = threading.RLock()
        self._subscribed_symbols: set[str] = set()
        self._trade_consumers: list[Callable] = []
        self._quote_consumers: list[Callable] = []
        self._consumer_errors = 0
        self._trade_consumer_errors = 0
        self._quote_consumer_errors = 0
        self._last_consumer_error = ""
        self._last_event_kind = ""
        self._last_event_symbol = ""
        self._last_event_ts: float | None = None
        self._last_subscription_request_ts: float | None = None
        self._server_subscription_state: dict[str, list[str]] = {}
        self._last_subscription_ack_ts: float | None = None

    def _feed(self):
        if self.feed_name == "sip":
            return DataFeed.SIP
        if self.feed_name == "delayed_sip":
            return DataFeed.DELAYED_SIP
        return DataFeed.IEX

    def add_consumer(self, trade_callback: Callable | None = None, quote_callback: Callable | None = None) -> None:
        """Registra consumidores del stream compartido sin abrir otra conexión."""
        with self._symbols_lock:
            if trade_callback is not None and trade_callback not in self._trade_consumers:
                self._trade_consumers.append(trade_callback)
            if quote_callback is not None and quote_callback not in self._quote_consumers:
                self._quote_consumers.append(quote_callback)

    def remove_consumer(self, trade_callback: Callable | None = None, quote_callback: Callable | None = None) -> None:
        with self._symbols_lock:
            if trade_callback in self._trade_consumers:
                self._trade_consumers.remove(trade_callback)
            if quote_callback in self._quote_consumers:
                self._quote_consumers.remove(quote_callback)

    def _record_event(self, kind: str, symbol: str) -> None:
        with self._symbols_lock:
            self._last_event_kind = kind
            self._last_event_symbol = str(symbol or "").upper()
            self._last_event_ts = time.time()

    async def _notify_consumers(self, callbacks: list[Callable], data, kind: str) -> None:
        for callback in callbacks:
            try:
                result = callback(data)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                with self._symbols_lock:
                    self._consumer_errors += 1
                    if kind == "trade":
                        self._trade_consumer_errors += 1
                    else:
                        self._quote_consumer_errors += 1
                    self._last_consumer_error = (
                        f"{kind} {getattr(callback, '__qualname__', type(callback).__name__)}: "
                        f"{type(exc).__name__}: {exc}"
                    )[:500]

    async def _quote(self, data) -> None:
        symbol = getattr(data, "symbol", "")
        self.cache.update_quote(
            symbol,
            getattr(data, "bid_price", None),
            getattr(data, "ask_price", None),
            getattr(data, "timestamp", None),
        )
        self.health.mark_quote(symbol)
        self._record_event("quote", symbol)
        with self._symbols_lock:
            callbacks = list(self._quote_consumers)
        await self._notify_consumers(callbacks, data, "quote")

    async def _trade(self, data) -> None:
        symbol = getattr(data, "symbol", "")
        price = getattr(data, "price", None)
        size = getattr(data, "size", 0)
        ts = getattr(data, "timestamp", None)
        self.cache.update_trade(symbol, price, size, ts)
        self.health.mark_trade(symbol)
        self._record_event("trade", symbol)
        try:
            bars = self.bars.on_trade(symbol, price, size, ts)
            if self.on_bar is not None:
                for bar in bars:
                    try:
                        self.on_bar(bar)
                    except Exception as exc:
                        self.health.mark_error(f"on_bar {symbol}: {type(exc).__name__}: {exc}")
        except Exception as exc:
            self.health.mark_error(f"bar builder {symbol}: {type(exc).__name__}: {exc}")
        with self._symbols_lock:
            callbacks = list(self._trade_consumers)
        await self._notify_consumers(callbacks, data, "trade")

    def _sync_subscriptions(self, stream) -> set[str] | None:
        """Serializa las llamadas de suscripción y reconcilia el estado deseado."""
        with self._subscription_lock:
            if self._stream is not stream:
                return None
            with self._symbols_lock:
                requested = set(self._symbols)
            current = set(self._subscribed_symbols)
            add = requested - current
            remove = current - requested
            add_started = False
            remove_started = False
            try:
                if add:
                    symbols = sorted(add)
                    add_started = True
                    stream.subscribe_quotes(self._quote, *symbols)
                    stream.subscribe_trades(self._trade, *symbols)
                    self._subscribed_symbols.update(add)
                if remove:
                    symbols = sorted(remove)
                    remove_started = True
                    stream.unsubscribe_quotes(*symbols)
                    stream.unsubscribe_trades(*symbols)
                    self._subscribed_symbols.difference_update(remove)
                if requested:
                    with self._symbols_lock:
                        self._last_subscription_request_ts = time.time()
            except Exception as exc:
                # SDK calls may fail after partially changing server-side
                # subscriptions. Compensate BOTH event types independently.
                rollback_errors = []
                if add_started:
                    for operation, symbols in (
                        (stream.unsubscribe_quotes, sorted(add)),
                        (stream.unsubscribe_trades, sorted(add)),
                    ):
                        try:
                            operation(*symbols)
                        except Exception as rollback_exc:
                            rollback_errors.append(rollback_exc)
                if remove_started:
                    for operation, symbols in (
                        (lambda *items: stream.subscribe_quotes(self._quote, *items), sorted(remove)),
                        (lambda *items: stream.subscribe_trades(self._trade, *items), sorted(remove)),
                    ):
                        try:
                            operation(*symbols)
                        except Exception as rollback_exc:
                            rollback_errors.append(rollback_exc)
                for rollback_exc in rollback_errors:
                    self.health.mark_error(
                        f"subscription rollback failed: {type(rollback_exc).__name__}: {rollback_exc}"
                    )
                self.health.mark_error(exc)
                return None
            return requested

    def _instrument_stream_dispatch(self, stream) -> None:
        """Record server-side websocket errors before the SDK only logs them."""
        dispatch = getattr(stream, "_dispatch", None)
        if not callable(dispatch):
            return

        async def dispatch_with_diagnostics(message):
            if isinstance(message, dict) and message.get("T") == "error":
                code = message.get("code", "unknown")
                detail = str(message.get("msg") or "sin detalle")
                self.health.mark_error(f"Alpaca websocket error {code}: {detail}"[:500])
            elif isinstance(message, dict) and message.get("T") == "subscription":
                with self._symbols_lock:
                    self._server_subscription_state = {
                        str(key): [str(symbol) for symbol in (value or [])]
                        for key, value in message.items()
                        if key != "T" and isinstance(value, (list, tuple))
                    }
                    self._last_subscription_ack_ts = time.time()
            await dispatch(message)

        # StockDataStream's dispatcher is the point where Alpaca's error frames
        # are otherwise only written to the SDK logger and not exposed to the UI.
        stream._dispatch = dispatch_with_diagnostics

    def _run(self) -> None:
        self.health.start(self.feed_name)
        while not self._stop.is_set():
            stream = None
            try:
                # No mostrar el ACK de la conexión anterior mientras la nueva
                # todavía no confirma qué símbolos aceptó el servidor.
                with self._symbols_lock:
                    self._server_subscription_state = {}
                    self._last_subscription_ack_ts = None
                stream = StockDataStream(
                    self.api_key,
                    self.secret_key,
                    feed=self._feed(),
                    data_timeout=60,
                )
                self._instrument_stream_dispatch(stream)
                # Publish the stream and reconcile desired symbols atomically
                # against concurrent start()/update_symbols() calls.
                with self._subscription_lock:
                    self._stream = stream
                    self._subscribed_symbols.clear()
                    with self._symbols_lock:
                        has_symbols = bool(self._symbols)
                if not has_symbols:
                    with self._subscription_lock:
                        if self._stream is stream:
                            self._stream = None
                    return
                symbols = self._sync_subscriptions(stream)
                if symbols is None:
                    with self._symbols_lock:
                        still_desired = bool(self._symbols)
                    if still_desired:
                        raise RuntimeError("alpaca_subscription_setup_failed")
                    return
                if not symbols:
                    with self._symbols_lock:
                        still_desired = bool(self._symbols)
                    if still_desired:
                        # A synchronous subscription failure must not leave a
                        # live-but-idle websocket. Let the outer loop close it
                        # and retry with a fresh SDK connection.
                        raise RuntimeError("alpaca_subscription_setup_failed")
                    with self._subscription_lock:
                        if self._stream is stream:
                            self._stream = None
                    return
                # La conexión real se confirma al recibir el primer evento.
                stream.run()
                if not self._stop.is_set():
                    self.health.mark_error("StockDataStream.run() terminó; reconectando")
            except Exception as exc:
                if not self._stop.is_set():
                    self.health.mark_error(exc)
            finally:
                # Release the previous SDK websocket before retrying. This avoids
                # stale sessions consuming Alpaca's connection budget on reconnect.
                if stream is not None:
                    try:
                        stream.stop()
                    except Exception:
                        pass
                with self._subscription_lock:
                    self._subscribed_symbols.clear()
                    if self._stream is stream:
                        self._stream = None
                with self.health._lock:
                    self.health.connected = False
            if not self._stop.wait(1.0):
                continue

    def _normalizar_simbolos(self, symbols: Iterable[str]) -> list[str]:
        """Normaliza y limita sin perder el orden de prioridad del llamador."""
        ordered = []
        seen = set()
        for symbol in symbols or []:
            ticker = str(symbol or "").strip().upper()
            if ticker and ticker not in seen:
                seen.add(ticker)
                ordered.append(ticker)
        return ordered[:self.max_symbols]

    def start(self, symbols: Iterable[str]) -> None:
        requested = set(self._normalizar_simbolos(symbols))
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
            self._symbols = set(requested)
            stream = self._stream
        # If connection setup is still in progress, _run() will reconcile the
        # latest desired set before entering run(). Otherwise reconcile now.
        if stream is not None:
            reconciled = self._sync_subscriptions(stream)
            if reconciled is None:
                # A failed live update must also trigger the worker's reconnect
                # loop; otherwise the websocket could remain alive while the
                # newly requested symbols never receive both event types.
                try:
                    stream.stop()
                except Exception as exc:
                    self.health.mark_error(f"stop after subscription failure: {type(exc).__name__}: {exc}")

    def update_symbols(self, symbols: Iterable[str]) -> None:
        requested = set(self._normalizar_simbolos(symbols))
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
        # Never drop the thread handle while the SDK run loop is still alive.
        # Otherwise a subsequent start() could create a second websocket and
        # consume the account's connection/subscription budget.
        if thread is not None and thread.is_alive():
            return
        self._thread = None
        with self._subscription_lock:
            self._stream = None
            self._subscribed_symbols.clear()

    def health_snapshot(self) -> dict:
        snapshot = self.health.snapshot()
        # _subscribed_symbols is owned by _subscription_lock, not _symbols_lock.
        # Copy it under its owner lock so a concurrent subscribe/unsubscribe
        # cannot mutate the set while the UI sorts it.
        with self._subscription_lock:
            subscribed_symbols = sorted(self._subscribed_symbols)
        with self._symbols_lock:
            snapshot.update({
                "running": bool(self._thread is not None and self._thread.is_alive()),
                "subscribed_symbols": subscribed_symbols,
                "consumer_errors": int(self._consumer_errors),
                "trade_consumer_errors": int(self._trade_consumer_errors),
                "quote_consumer_errors": int(self._quote_consumer_errors),
                "last_consumer_error": self._last_consumer_error,
                "last_event_kind": self._last_event_kind,
                "last_event_symbol": self._last_event_symbol,
                "last_event_ts": self._last_event_ts,
                "last_event_age_sec": (
                    max(0.0, time.time() - self._last_event_ts)
                    if self._last_event_ts is not None else None
                ),
                "last_subscription_request_ts": self._last_subscription_request_ts,
                "server_subscription_state": {
                    key: list(value) for key, value in self._server_subscription_state.items()
                },
                "last_subscription_ack_ts": self._last_subscription_ack_ts,
            })
        return snapshot
