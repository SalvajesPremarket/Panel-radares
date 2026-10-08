"""Private TradeScanner signal API. No scanner or broker secrets are exposed."""
import os
from urllib.parse import urlparse
from fastapi import FastAPI, Depends, HTTPException, Query
from webapp.auth.server import get_current_user
from webapp.api.signal_service import store

app=FastAPI(title="TradeScanner Private API",version="0.1.0")

def commercial_user(user=Depends(get_current_user)):
    if user["account_status"] not in {"trial","active_monthly","active_annual","admin"}:
        raise HTTPException(status_code=403,detail="Acceso comercial no activo")
    return user

def scanner_ui_url():
    value = str(os.getenv("TRADESCANNER_SCANNER_URL", "") or "").strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return value.rstrip("/")

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
    }

@app.get("/api/v1/signals")
def signals(since: str|None=None,timeframe: str|None=None,symbol: str|None=None,
            limit:int=Query(default=100,ge=1,le=500),user=Depends(commercial_user)):
    return {"items":store.list(since=since,timeframe=timeframe,symbol=symbol,limit=limit)}
