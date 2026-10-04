"""Signal-only and paper-trading engine. No real broker calls."""
from dataclasses import dataclass,asdict
from datetime import datetime,timezone
from threading import Lock
from uuid import uuid4

@dataclass
class RiskConfig:
    max_capital_per_trade: float=1000.0
    max_risk_per_trade: float=0.01
    max_simultaneous_positions: int=3
    daily_loss_limit: float=0.03
    max_exposure: float=0.20
    stop_loss_pct: float=0.02
    take_profit_pct: float=0.04
    kill_switch: bool=False

@dataclass
class Decision:
    decision_id:str
    signal_id:str
    mode:str
    action:str
    reason:str
    created_at:str

class PaperBot:
    def __init__(self,risk=None):
        self.risk=risk or RiskConfig()
        self.positions={}
        self.decisions=[]
        self._lock=Lock()

    def evaluate(self,signal):
        with self._lock:
            now=datetime.now(timezone.utc).isoformat()
            if self.risk.kill_switch: return self._record(signal,"blocked","kill_switch",now)
            if len(self.positions)>=self.risk.max_simultaneous_positions:
                return self._record(signal,"blocked","max_simultaneous_positions",now)
            kind=signal["signal_type"].upper()
            action="buy" if kind in {"LONG","BUY"} else "sell" if kind in {"SHORT","SELL"} else "watch"
            return self._record(signal,action,"risk_checks_passed" if action!="watch" else "signal_not_tradeable",now)

    def _record(self,signal,action,reason,now):
        d=Decision(uuid4().hex,signal["signal_id"],"paper",action,reason,now)
        self.decisions.append(asdict(d)); self.decisions=self.decisions[-1000:]
        return asdict(d)
