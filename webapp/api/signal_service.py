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


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def confidence_score(item: dict) -> tuple[int, dict]:
    """Calculate a 0-100 technical confidence score without changing filters."""
    long_side = bool(item.get("macd_positivo") or item.get("cruzando_ema20"))
    if item.get("macd_negativo") or item.get("cruzando_ema20_abajo"):
        long_side = False
    components = {}
    ema_confirmed = bool(item.get("cruce_ema20_confirmado"))
    ema_cross = bool(item.get("cruzando_ema20" if long_side else "cruzando_ema20_abajo"))
    components["ema_cross"] = 30 if ema_confirmed else (20 if ema_cross else 0)
    macd_positive = bool(item.get("macd_positivo"))
    macd_negative = bool(item.get("macd_negativo"))
    if (long_side and macd_positive) or ((not long_side) and macd_negative):
        components["macd"] = 24
        macd_value = _as_float(item.get("tecnico_macd"))
        if macd_value is not None and ((long_side and macd_value > 0) or ((not long_side) and macd_value < 0)):
            components["macd"] = 30
    else:
        components["macd"] = 0 if ((long_side and macd_negative) or ((not long_side) and macd_positive)) else 12
    price = _as_float(item.get("tecnico_precio") or item.get("precio"))
    ema20 = _as_float(item.get("tecnico_ema20"))
    if price is not None and ema20 not in (None, 0):
        distance_pct = abs((price - ema20) / ema20) * 100
        aligned = price >= ema20 if long_side else price <= ema20
        components["price_vs_ema20"] = 15 if aligned and distance_pct >= 1 else (12 if aligned and distance_pct >= 0.25 else (7 if aligned else 0))
    else:
        components["price_vs_ema20"] = 0
    ema50 = _as_float(item.get("ema50"))
    ema200 = _as_float(item.get("ema200"))
    if price is not None and ema20 not in (None, 0) and ema50 is not None and ema200 is not None:
        aligned = (price > ema20 > ema50 > ema200) if long_side else (price < ema20 < ema50 < ema200)
        partial = (price > ema20 and ema20 > ema50) if long_side else (price < ema20 and ema20 < ema50)
        components["ema_trend"] = 15 if aligned else (8 if partial else 0)
    else:
        components["ema_trend"] = 0
    rsi = _as_float(item.get("rsi"))
    if rsi is None:
        components["rsi"] = 0
    elif long_side:
        components["rsi"] = 5 if 50 <= rsi <= 65 else (3 if 40 <= rsi < 50 or 65 < rsi <= 70 else (2 if 30 <= rsi < 40 else 0))
    else:
        components["rsi"] = 5 if 35 <= rsi <= 50 else (3 if 30 <= rsi < 35 or 50 < rsi <= 60 else (2 if 60 < rsi <= 70 else 0))
    required = [price, ema20, ema50, ema200, rsi]
    components["data_quality"] = 5 if all(value is not None for value in required) else (3 if sum(value is not None for value in required) >= 3 else 0)
    total = max(0, min(100, sum(components.values())))
    if total >= 90:
        label = "EXCELENTE"
    elif total >= 80:
        label = "MUY FUERTE"
    elif total >= 70:
        label = "FUERTE"
    elif total >= 60:
        label = "MODERADA"
    elif total >= 50:
        label = "DÉBIL"
    else:
        label = "MUY DÉBIL"
    return total, {"label": label, "components": components}
