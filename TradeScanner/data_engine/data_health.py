"""Salud operacional del canal de datos en tiempo real."""

from __future__ import annotations

import threading
import time
from typing import Any


class DataHealth:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.connected = False
        self.feed = ""
        self.started_ts: float | None = None
        self.last_event_ts: float | None = None
        self.quotes = 0
        self.trades = 0
        self.errors = 0
        self.last_error = ""
        self.symbols_seen: set[str] = set()

    def start(self, feed: str) -> None:
        with self._lock:
            self.connected = False
            self.feed = str(feed or "")
            self.started_ts = time.time()
            self.last_error = ""

    def mark_connected(self) -> None:
        with self._lock:
            self.connected = True

    def mark_quote(self, symbol: str) -> None:
        with self._lock:
            self.quotes += 1
            self.last_event_ts = time.time()
            if symbol:
                self.symbols_seen.add(str(symbol).upper())

    def mark_trade(self, symbol: str) -> None:
        with self._lock:
            self.trades += 1
            self.last_event_ts = time.time()
            if symbol:
                self.symbols_seen.add(str(symbol).upper())

    def mark_error(self, error: Any) -> None:
        with self._lock:
            self.errors += 1
            self.last_error = str(error)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            age = None if self.last_event_ts is None else max(0.0, time.time() - self.last_event_ts)
            return {
                "connected": self.connected,
                "feed": self.feed,
                "started_ts": self.started_ts,
                "last_event_ts": self.last_event_ts,
                "last_event_age": age,
                "quotes": self.quotes,
                "trades": self.trades,
                "errors": self.errors,
                "last_error": self.last_error,
                "symbols_seen": len(self.symbols_seen),
            }
