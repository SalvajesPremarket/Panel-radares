"""Cache thread-safe de la última cotización/trade recibida por símbolo."""

from __future__ import annotations

import threading
import time
from typing import Any


class MarketCache:
    """Estado de mercado compartido entre stream, builder y scanner.

    No contiene lógica de trading. Solo conserva el último evento válido.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._quotes: dict[str, dict[str, Any]] = {}
        self._trades: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _symbol(symbol: Any) -> str:
        return str(symbol or "").strip().upper()

    def update_quote(self, symbol: Any, bid: Any, ask: Any, ts: Any = None) -> None:
        key = self._symbol(symbol)
        if not key:
            return
        with self._lock:
            self._quotes[key] = {
                "symbol": key,
                "bid": bid,
                "ask": ask,
                "ts": ts,
                "received_ts": time.time(),
            }

    def update_trade(self, symbol: Any, price: Any, size: Any = None, ts: Any = None) -> None:
        key = self._symbol(symbol)
        if not key:
            return
        with self._lock:
            self._trades[key] = {
                "symbol": key,
                "price": price,
                "size": size,
                "ts": ts,
                "received_ts": time.time(),
            }

    def quote(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            value = self._quotes.get(self._symbol(symbol))
            return dict(value) if value else None

    def trade(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            value = self._trades.get(self._symbol(symbol))
            return dict(value) if value else None

    def snapshot(self) -> dict[str, dict[str, dict[str, Any]]]:
        with self._lock:
            return {
                "quotes": {k: dict(v) for k, v in self._quotes.items()},
                "trades": {k: dict(v) for k, v in self._trades.items()},
            }

    def symbols(self) -> set[str]:
        with self._lock:
            return set(self._quotes) | set(self._trades)
