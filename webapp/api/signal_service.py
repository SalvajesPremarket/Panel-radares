"""Private scanner signal adapter. The existing app.py remains untouched."""
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from threading import Lock
from uuid import uuid4

@dataclass
class Signal:
    symbol: str
    timeframe: str
    signal_type: str
    price: float
    timestamp: str
    source: str = "TradeScanner"
    confidence: float | None = None
    scanner_conditions: dict | None = None
    risk_context: dict | None = None
    signal_id: str = ""

    def __post_init__(self):
        if not self.signal_id:
            self.signal_id = uuid4().hex

class SignalStore:
    def __init__(self):
        self._signals=[]
        self._lock=Lock()

    def publish(self, signal):
        with self._lock:
            self._signals.append(signal)
            self._signals=self._signals[-1000:]
        return signal

    def list(self, since=None, timeframe=None, symbol=None, limit=100):
        with self._lock:
            items=list(reversed(self._signals))
        if since: items=[x for x in items if x.timestamp >= since]
        if timeframe: items=[x for x in items if x.timeframe == timeframe]
        if symbol: items=[x for x in items if x.symbol.upper() == symbol.upper()]
        return [asdict(x) for x in items[:max(1,min(limit,500))]]

store=SignalStore()

def publish_signal(symbol,timeframe,signal_type,price,confidence=None,scanner_conditions=None,risk_context=None):
    return store.publish(Signal(symbol.upper(),timeframe,signal_type,float(price),
        datetime.now(timezone.utc).isoformat(),confidence=confidence,
        scanner_conditions=scanner_conditions,risk_context=risk_context))
