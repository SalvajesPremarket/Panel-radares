"""External publisher for real TradeScanner final signals.

This module is deliberately isolated from the scanner logic. It only forwards
signals that the existing scanner has already accepted as final results.
"""
import os
import threading
from typing import Iterable

import requests

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
            fingerprint = (symbol, str(timeframe), signal_type, round(price_value, 4))
            if fingerprint in _last_sent:
                continue
            _last_sent.add(fingerprint)
            batch.append({
                "symbol": symbol,
                "timeframe": str(timeframe),
                "signal_type": signal_type,
                "price": price_value,
                "confidence": None,
                "scanner_conditions": {
                    "gap_pct": item.get("gap_pct"),
                    "cruzando_ema20": bool(item.get("cruzando_ema20")),
                    "macd_positivo": bool(item.get("macd_positivo")),
                    "float_status": item.get("float_status"),
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
