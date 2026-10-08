"""External publisher for real TradeScanner final signals.

This module is deliberately isolated from the scanner logic. It only forwards
signals that the existing scanner has already accepted as final results.
"""
import os
import threading
from typing import Iterable

import requests


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _confidence_score(item: dict) -> tuple[int, dict]:
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


_lock = threading.Lock()
_last_sent = set()
_session = requests.Session()


def publish_final_signals(items: Iterable[dict], timeframe: str) -> int:
    endpoint = os.getenv("TRADESCANNER_SIGNAL_INGEST_URL", "https://tradescanner-webapp.onrender.com/api/v1/signals/ingest").strip()
    secret = os.getenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "").strip()
    if not endpoint or not secret:
        return 0

    batch = []
    with _lock:
        for item in items:
            symbol = str(item.get("ticker") or "").strip().upper()
            price = item.get("precio")
            if not symbol or price is None:
                continue
            try:
                price_value = float(price)
            except (TypeError, ValueError):
                continue

            signal_type = "SCANNER_FINAL"
            confidence, confidence_detail = _confidence_score(item)
            fingerprint = (symbol, str(timeframe), signal_type, round(price_value, 4))
            if fingerprint in _last_sent:
                continue
            _last_sent.add(fingerprint)
            batch.append({
                "symbol": symbol,
                "timeframe": str(timeframe),
                "signal_type": signal_type,
                "price": price_value,
                "confidence": confidence,
                "scanner_conditions": {
                    "gap_pct": item.get("gap_pct"),
                    "cruzando_ema20": bool(item.get("cruzando_ema20")),
                    "macd_positivo": bool(item.get("macd_positivo")),
                    "float_status": item.get("float_status"),
                    "confidence_label": confidence_detail["label"],
                    "confidence_components": confidence_detail["components"],
                },
                "risk_context": None,
            })

        # Keep the in-process dedupe cache bounded.
        if len(_last_sent) > 5000:
            _last_sent.clear()

    if not batch:
        return 0

    try:
        response = _session.post(
            endpoint,
            json={"items": batch},
            headers={"X-TradeScanner-Signal-Key": secret},
            timeout=3,
        )
        response.raise_for_status()
        return len(batch)
    except Exception as exc:
        # Publishing failure must never stop or alter the scanner cycle.
        print(f"⚠️ Signal publisher: {type(exc).__name__}: {exc}")
        return 0
