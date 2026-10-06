"""Constructor de velas en memoria a partir de trades en tiempo real."""

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class LiveBar:
    symbol: str
    start_ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    trades: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "start_ts": self.start_ts,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "trades": self.trades,
        }


class LiveBarBuilder:
    """Construye barras 20s/1m sin depender de pandas ni de Streamlit."""

    def __init__(self, intervals: tuple[int, ...] = (20, 60)) -> None:
        self.intervals = tuple(sorted({int(x) for x in intervals if int(x) > 0}))
        self._lock = threading.RLock()
        self._bars: dict[tuple[str, int], LiveBar] = {}

    @staticmethod
    def _epoch_seconds(ts: Any) -> float:
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts.timestamp()
        if ts is None:
            return datetime.now(timezone.utc).timestamp()
        try:
            return float(ts)
        except (TypeError, ValueError):
            return datetime.now(timezone.utc).timestamp()

    def on_trade(self, symbol: str, price: Any, size: Any = 0, ts: Any = None) -> list[LiveBar]:
        symbol = str(symbol or "").strip().upper()
        if not symbol:
            return []
        try:
            px = float(price)
        except (TypeError, ValueError):
            return []
        if px <= 0:
            return []

        epoch = self._epoch_seconds(ts)
        size_f = 0.0
        try:
            size_f = max(0.0, float(size or 0))
        except (TypeError, ValueError):
            pass

        updated: list[LiveBar] = []
        with self._lock:
            for seconds in self.intervals:
                start = int(epoch // seconds) * seconds
                key = (symbol, seconds)
                bar = self._bars.get(key)
                if bar is None or bar.start_ts != start:
                    bar = LiveBar(symbol, start, px, px, px, px)
                    self._bars[key] = bar
                else:
                    bar.high = max(bar.high, px)
                    bar.low = min(bar.low, px)
                    bar.close = px
                bar.volume += size_f
                bar.trades += 1
                updated.append(LiveBar(**bar.__dict__))
        return updated

    def get(self, symbol: str, seconds: int) -> LiveBar | None:
        key = (str(symbol or "").strip().upper(), int(seconds))
        with self._lock:
            bar = self._bars.get(key)
            return LiveBar(**bar.__dict__) if bar else None
