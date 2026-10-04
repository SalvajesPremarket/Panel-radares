"""Bot API for signal and paper modes. Live broker execution is not implemented."""
from fastapi import FastAPI,Depends,HTTPException
from pydantic import BaseModel
from webapp.auth.server import get_current_user
from webapp.api.signal_service import store
from BotTradeScanner.riesgo.paper import PaperBot

app=FastAPI(title="TradeScanner Bot API",version="0.1.0")
bot=PaperBot()

def commercial_user(user=Depends(get_current_user)):
    if user["account_status"] not in {"trial","active_monthly","active_annual","admin"}:
        raise HTTPException(status_code=403,detail="Acceso comercial no activo")
    return user

class DecisionRequest(BaseModel):
    signal_id:str

@app.get("/api/v1/bot/status")
def status(user=Depends(commercial_user)):
    return {"authorized":True,"mode":"paper","live_execution":False}

@app.post("/api/v1/bot/decisions")
def decision(payload:DecisionRequest,user=Depends(commercial_user)):
    signal=next((x for x in store.list(limit=500) if x["signal_id"]==payload.signal_id),None)
    if not signal: raise HTTPException(status_code=404,detail="Señal no encontrada")
    return bot.evaluate(signal)
