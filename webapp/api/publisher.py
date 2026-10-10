"""External publisher for real TradeScanner final signals.

Only forwards results already accepted by the scanner's existing filters.
A failed HTTP delivery is not remembered as sent, so a later cycle can retry.
"""
import os
import threading
from typing import Iterable

import requests

from webapp.api.signal_service import confidence_score


_lock = threading.Lock()
_last_sent = set()
_session = requests.Session()


def _number(item: dict, *keys):
    for key in keys:
        try:
            value = item.get(key)
            if value is not None and value != "":
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _market_context(item: dict, motor_bridge, symbol: str) -> dict:
    """Include higher-timeframe EMA context when the scanner motor has it."""
    snapshot = {}
    if motor_bridge is not None:
        try:
            snapshot = motor_bridge.snapshot(symbol) or {}
        except Exception:
            snapshot = {}

    context = {
        "ema20": _number(item, "tecnico_ema20", "tecnico_ema20_actual", "ema20"),
        "ema50_dia": _number(item, "ema50_dia", "ema50_diaria", "ema50_daily"),
        "ema200_dia": _number(item, "ema200_dia", "ema200_diaria", "ema200_daily"),
        "ema50_semana": _number(item, "ema50_semana", "ema50_semanal", "ema50_weekly"),
        "ema200_semana": _number(item, "ema200_semana", "ema200_semanal", "ema200_weekly"),
    }
    snapshot_aliases = {
        "ema20": ("ema20",),
        "ema50_dia": ("ema50_diaria", "ema50_dia", "ema50_daily"),
        "ema200_dia": ("ema200_diaria", "ema200_dia", "ema200_daily"),
        "ema50_semana": ("ema50_semanal", "ema50_semana", "ema50_weekly"),
        "ema200_semana": ("ema200_semanal", "ema200_semana", "ema200_weekly"),
    }
    for target, aliases in snapshot_aliases.items():
        if context[target] is None:
            context[target] = _number(snapshot, *aliases)
    return {key: value for key, value in context.items() if value is not None}


def publish_final_signals(items: Iterable[dict], timeframe: str, motor_bridge=None) -> int:
    endpoint = os.getenv(
        "TRADESCANNER_SIGNAL_INGEST_URL",
        "https://tradescanner-webapp.onrender.com/api/v1/signals/ingest",
    ).strip()
    secret = os.getenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "").strip()
    if not endpoint or not secret:
        return 0

    batch = []
    fingerprints = []
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
            confidence, confidence_detail = confidence_score(item)
            fingerprint = (symbol, str(timeframe), signal_type, round(price_value, 4))
            if fingerprint in _last_sent:
                continue
            fingerprints.append(fingerprint)
            context = {
                "gap_pct": _number(item, "gap_pct"),
                "cruzando_ema20": bool(item.get("cruzando_ema20")),
                "macd_positivo": bool(item.get("macd_positivo")),
                "float_status": item.get("float_status"),
                "confidence_label": confidence_detail["label"],
                "confidence_components": confidence_detail["components"],
            }
            context.update(_market_context(item, motor_bridge, symbol))
            batch.append({
                "symbol": symbol,
                "timeframe": str(timeframe),
                "signal_type": signal_type,
                "price": price_value,
                "confidence": confidence,
                "scanner_conditions": context,
                "risk_context": None,
            })

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
        with _lock:
            _last_sent.update(fingerprints)
            if len(_last_sent) > 5000:
                _last_sent.clear()
                _last_sent.update(fingerprints)
        return len(batch)
    except Exception as exc:
        # Failed delivery must be retryable and must never stop the scanner cycle.
        print(f"⚠️ Signal publisher: {type(exc).__name__}: {exc}")
        return 0
