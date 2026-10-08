"""Adapter boundary for the real scanner.

This module does not import app.py. A future integration point can pass the already-running ServicioScanner instance to publish_service_results().
"""
from webapp.api.signal_service import publish_signal, confidence_score

def _signal_type(row):
    if row.get("macd_positivo") and row.get("cruzando_ema20"):
        return "LONG"
    if row.get("macd_negativo") and row.get("cruzando_ema20_abajo"):
        return "SHORT"
    return "WATCH"

def publish_service_results(servicio, timeframe=None):
    """Publish the current scanner result set without changing its filters."""
    tf = timeframe or getattr(servicio, "tf_principal", None) or "1m"
    rows = list(getattr(servicio, "resultados", []) or [])
    published = []
    for row in rows:
        symbol = row.get("ticker") or row.get("symbol")
        price = row.get("precio") or row.get("price")
        if not symbol or price is None:
            continue
        confidence, confidence_detail = confidence_score(row)
        published.append(publish_signal(
            symbol=symbol,
            timeframe=str(row.get("tecnico_timeframe") or tf),
            signal_type=_signal_type(row),
            price=price,
            confidence=confidence,
            scanner_conditions={"ema20_cross": bool(row.get("cruzando_ema20")), "macd_positive": bool(row.get("macd_positivo")), "confidence_label": confidence_detail["label"], "confidence_components": confidence_detail["components"]},
            risk_context={"source_result": "ServicioScanner.resultados"},
        ))
    return published
