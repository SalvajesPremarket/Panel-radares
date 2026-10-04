"""Gestion de Paper Trading para PreMarketSalvajes LONG.

No envia ordenes reales. La estrategia determina stop y salidas.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from threading import Lock
from uuid import uuid4


@dataclass
class RiskConfig:
    max_capital_per_trade: float = 1000.0
    max_risk_per_trade: float = 0.01
    max_simultaneous_positions: int = 3
    daily_loss_limit: float = 0.03
    max_exposure: float = 0.20
    kill_switch: bool = False


@dataclass
class PaperPosition:
    position_id: str
    simbolo: str
    estrategia: str
    precio_entrada: float
    stop_loss: float | None
    cantidad: float | None
    opened_at: str


@dataclass
class Decision:
    decision_id: str
    signal_id: str
    mode: str
    action: str
    reason: str
    created_at: str
    simbolo: str = ""
    precio: float | None = None
    stop_loss: float | None = None
    position_id: str | None = None


class PaperBot:
    """Simulador del ciclo BUY -> posicion -> HOLD -> EXIT."""

    def __init__(self, risk: RiskConfig | None = None):
        self.risk = risk or RiskConfig()
        self.positions: dict[str, PaperPosition] = {}
        self.decisions: list[dict] = []
        self._lock = Lock()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _signal_id(signal: dict) -> str:
        return str(signal.get("signal_id") or uuid4().hex)

    @staticmethod
    def _simbolo(signal: dict) -> str:
        return str(signal.get("simbolo") or signal.get("ticker") or "").strip().upper()

    @staticmethod
    def _precio(signal: dict) -> float | None:
        value = signal.get("precio", signal.get("price"))
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def evaluar(self, signal: dict) -> dict:
        with self._lock:
            now = self._now()
            signal_id = self._signal_id(signal)
            simbolo = self._simbolo(signal)
            accion = str(signal.get("accion") or "").upper()
            precio = self._precio(signal)
            try:
                stop_loss = float(signal["stop_loss"]) if signal.get("stop_loss") is not None else None
            except (TypeError, ValueError):
                stop_loss = None

            if not simbolo:
                return self._record(signal_id, "blocked", "signal_without_symbol", now)

            if self.risk.kill_switch:
                return self._record(signal_id, "blocked", "kill_switch", now, simbolo, precio, stop_loss)

            if accion == "BUY":
                return self._open(signal_id, simbolo, precio, stop_loss, signal, now)
            if accion == "EXIT":
                return self._close(signal_id, simbolo, precio, signal, now)
            if accion in {"HOLD", "WATCH", "WAIT"}:
                pos = self.positions.get(simbolo)
                return self._record(
                    signal_id, accion.lower(), str(signal.get("motivo") or accion.lower()), now,
                    simbolo, precio, stop_loss, pos.position_id if pos else None
                )
            return self._record(signal_id, "ignored", f"unsupported_action:{accion}", now, simbolo, precio, stop_loss)

    def _open(self, signal_id, simbolo, precio, stop_loss, signal, now):
        if precio is None:
            return self._record(signal_id, "blocked", "BUY_without_price", now, simbolo, None, stop_loss)
        if simbolo in self.positions:
            p = self.positions[simbolo]
            return self._record(signal_id, "hold", "position_already_open", now, simbolo, precio, p.stop_loss, p.position_id)
        if len(self.positions) >= self.risk.max_simultaneous_positions:
            return self._record(signal_id, "blocked", "max_simultaneous_positions", now, simbolo, precio, stop_loss)
        if stop_loss is not None and stop_loss >= precio:
            return self._record(signal_id, "blocked", "invalid_long_stop", now, simbolo, precio, stop_loss)

        position_id = uuid4().hex
        self.positions[simbolo] = PaperPosition(
            position_id, simbolo,
            str(signal.get("estrategia") or "PreMarketSalvajes LONG"),
            precio, stop_loss, None, now
        )
        return self._record(signal_id, "buy", "paper_position_opened", now, simbolo, precio, stop_loss, position_id)

    def _close(self, signal_id, simbolo, precio, signal, now):
        position = self.positions.pop(simbolo, None)
        if position is None:
            return self._record(signal_id, "ignored", "exit_without_open_position", now, simbolo, precio, signal.get("stop_loss"))
        return self._record(
            signal_id, "sell", str(signal.get("motivo") or "strategy_exit"), now,
            simbolo, precio, position.stop_loss, position.position_id
        )

    def _record(self, signal_id, action, reason, now, simbolo="", precio=None, stop_loss=None, position_id=None):
        item = asdict(Decision(
            uuid4().hex, signal_id, "paper", action, reason, now,
            simbolo, precio, stop_loss, position_id
        ))
        self.decisions.append(item)
        self.decisions = self.decisions[-1000:]
        return item

    def posiciones(self) -> list[dict]:
        with self._lock:
            return [asdict(p) for p in self.positions.values()]

    def status(self) -> dict:
        with self._lock:
            return {
                "modo": "paper",
                "posiciones_abiertas": len(self.positions),
                "max_simultaneous_positions": self.risk.max_simultaneous_positions,
                "kill_switch": self.risk.kill_switch,
                "decisiones": len(self.decisions),
            }
