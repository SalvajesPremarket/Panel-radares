"""Private TradeScanner signal API. No scanner or broker secrets are exposed."""
import hmac
import os
from urllib.parse import urlparse

from fastapi import FastAPI, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from webapp.auth.server import get_current_user
from webapp.api.signal_service import store, publish_signal

app=FastAPI(title="TradeScanner Private API",version="0.1.0")

# Deployed Streamlit scanner. Can still be overridden by the server environment.
DEFAULT_SCANNER_URL = "https://jd6gih.streamlit.app"

class SignalIn(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    timeframe: str = Field(min_length=1, max_length=20)
    signal_type: str = Field(min_length=1, max_length=40)
    price: float
    confidence: float | None = Field(default=None, ge=0, le=1)
    scanner_conditions: dict | None = None
    risk_context: dict | None = None

class SignalBatchIn(BaseModel):
    items: list[SignalIn] = Field(min_length=1, max_length=500)

def commercial_user(user=Depends(get_current_user)):
    if user["account_status"] not in {"trial","active_monthly","active_annual","admin"}:
        raise HTTPException(status_code=403,detail="Acceso comercial no activo")
    return user

def scanner_ui_url():
    value = str(os.getenv("TRADESCANNER_SCANNER_URL", DEFAULT_SCANNER_URL) or "").strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return value.rstrip("/")

def require_ingest_key(x_tradescanner_signal_key: str | None = Header(default=None)):
    expected = os.getenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="Signal ingest is not configured")
    if not x_tradescanner_signal_key or not hmac.compare_digest(x_tradescanner_signal_key, expected):
        raise HTTPException(status_code=401, detail="Invalid signal ingest key")

@app.get("/api/v1/health")
def health():
    return {"ok":True,"service":"signals"}

@app.get("/api/v1/scanner/status")
def scanner_status(user=Depends(commercial_user)):
    live_url = scanner_ui_url()
    return {
        "authorized":True,
        "scanner":"available",
        "source":"private_signal_adapter",
        "app_integrity":"external_to_app.py",
        "live_ui_configured":bool(live_url),
        "live_ui_url":live_url,
        "signal_ingest_configured":bool(os.getenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "").strip()),
    }

@app.post("/api/v1/signals/ingest", status_code=202)
def ingest_signals(payload: SignalBatchIn, _: None = Depends(require_ingest_key)):
    published = []
    for item in payload.items:
        signal = publish_signal(
            symbol=item.symbol,
            timeframe=item.timeframe,
            signal_type=item.signal_type,
            price=item.price,
            confidence=item.confidence,
            scanner_conditions=item.scanner_conditions,
            risk_context=item.risk_context,
        )
        published.append(signal.signal_id)
    return {"accepted": len(published), "signal_ids": published}

@app.get("/api/v1/signals")
def signals(since: str|None=None,timeframe: str|None=None,symbol: str|None=None,
            limit:int=Query(default=100,ge=1,le=500),user=Depends(commercial_user)):
    return {"items":store.list(since=since,timeframe=timeframe,symbol=symbol,limit=limit)}
