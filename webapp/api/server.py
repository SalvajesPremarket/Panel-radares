"""Private TradeScanner signal API and paper-only TradeBot simulator."""
import hmac
import os
from typing import Literal
from threading import Lock
from urllib.parse import urlparse
from uuid import uuid4

from fastapi import FastAPI, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from BotTradeScanner.riesgo.paper import PaperBot
from webapp.auth.server import get_current_user
from webapp.api.signal_service import store, publish_signal

app=FastAPI(title="TradeScanner Private API",version="0.2.0")

# One process-local simulator per authenticated user; never creates a broker client.
_paper_bots: dict[str, PaperBot] = {}
_paper_bots_lock = Lock()

def paper_bot_for(user: dict) -> PaperBot:
    user_key = str(user.get("user_id") or user.get("email") or "").strip()
    if not user_key:
        raise HTTPException(status_code=401, detail="Identidad de usuario requerida")
    with _paper_bots_lock:
        if user_key not in _paper_bots:
            _paper_bots[user_key] = PaperBot()
        return _paper_bots[user_key]
# Deployed Streamlit scanner. Can still be overridden by the server environment.
DEFAULT_SCANNER_URL = "https://jd6gih.streamlit.app"

class SignalIn(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    timeframe: str = Field(min_length=1, max_length=20)
    signal_type: str = Field(min_length=1, max_length=40)
    price: float
    confidence: float | None = Field(default=None, ge=0, le=100)
    scanner_conditions: dict | None = None
    risk_context: dict | None = None

class SignalBatchIn(BaseModel):
    items: list[SignalIn] = Field(min_length=1, max_length=500)

class PaperTradeIn(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    action: Literal["BUY", "EXIT", "HOLD", "WATCH", "WAIT"]
    price: float = Field(gt=0)
    stop_loss: float | None = Field(default=None, gt=0)
    reason: str | None = Field(default=None, max_length=240)

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

@app.get("/api/v1/tradebot/status")
def tradebot_status(user=Depends(commercial_user)):
    """Expose honest runtime state; the strategy/feed loop is not auto-started."""
    bot = paper_bot_for(user)
    return {
        "mode": "paper",
        "paper_simulator_connected": True,
        "strategy_engine_connected": False,
        "strategy_engine_status": "market_data_feed_not_configured",
        "real_trading_enabled": False,
        "broker_connected": False,
        "status": bot.status(),
        "notice": "Simulador manual Paper activo; motor de mercado en tiempo real pendiente de configurar.",
    }

@app.get("/api/v1/tradebot/positions")
def tradebot_positions(user=Depends(commercial_user)):
    return {"items": paper_bot_for(user).posiciones()}

@app.get("/api/v1/tradebot/decisions")
def tradebot_decisions(limit: int = Query(default=50, ge=1, le=200), user=Depends(commercial_user)):
    bot = paper_bot_for(user)
    return {"items": list(reversed(bot.decisions[-limit:]))}

@app.post("/api/v1/tradebot/evaluate")
def tradebot_evaluate(payload: PaperTradeIn, user=Depends(commercial_user)):
    """Manually evaluate a simulated paper action. This endpoint has no broker access."""
    symbol = payload.symbol.strip().upper()
    if not symbol:
        raise HTTPException(status_code=422, detail="Símbolo requerido")
    if payload.action == "BUY" and payload.stop_loss is None:
        raise HTTPException(status_code=422, detail="Para una entrada simulada BUY debes indicar Stop Loss.")
    bot = paper_bot_for(user)
    result = bot.evaluar({
        "signal_id": uuid4().hex,
        "simbolo": symbol,
        "accion": payload.action,
        "precio": payload.price,
        "stop_loss": payload.stop_loss,
        "motivo": payload.reason or "paper_manual",
        "estrategia": "TradeBot Paper (manual)",
    })
    return {"mode": "paper", "real_trading_enabled": False, "result": result, "status": bot.status()}

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
