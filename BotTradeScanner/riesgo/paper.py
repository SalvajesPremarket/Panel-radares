"""Gestion de Paper Trading para PreMarketSalvajes LONG.

No envia ordenes reales. La estrategia determina stop y salidas.
El tamaño de posición se calcula por riesgo y queda limitado por exposición.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from math import floor
from threading import Lock
from uuid import uuid4


@dataclass
class RiskConfig:
    initial_capital: float = 600.0
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
    capital_expuesto: float
    riesgo_dolares: float
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
    cantidad: float | None = None
    capital_expuesto: float | None = None
    riesgo_dolares: float | None = None
    pnl_realizado: float | None = None


class PaperBot:
    """Simulador del ciclo BUY -> posicion -> HOLD -> EXIT."""

    def __init__(self, risk: RiskConfig | None = None):
        self.risk = risk or RiskConfig()
        self.positions: dict[str, PaperPosition] = {}
        self.decisions: list[dict] = []
        self.closed_trades: list[dict] = []
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

    @staticmethod
    def _fecha_operativa() -> str:
        # Fecha UTC para mantener el registro determinista del PaperBot.
        return datetime.now(timezone.utc).date().isoformat()

    def _pnl_hoy(self) -> float:
        hoy = self._fecha_operativa()
        return sum(
            float(trade["pnl_realizado"])
            for trade in self.closed_trades
            if trade["fecha"] == hoy
        )

    def _limite_perdida_diaria(self) -> float:
        return self.risk.initial_capital * self.risk.daily_loss_limit

    def _calcular_tamano(self, precio: float, stop_loss: float | None) -> tuple[float, float, float, str | None]:
        if stop_loss is None:
            return 0.0, 0.0, 0.0, "missing_long_stop"

        distancia_stop = precio - stop_loss
        if distancia_stop < 0:
            return 0.0, 0.0, 0.0, "invalid_long_stop"

        riesgo_maximo = self.risk.initial_capital * self.risk.max_risk_per_trade
        exposicion_maxima = self.risk.initial_capital * self.risk.max_exposure

        # Un stop exactamente en la entrada implica riesgo monetario cero.
        # En ese caso el tamaño queda limitado exclusivamente por exposicion.
        cantidad_por_riesgo = (
            float("inf") if distancia_stop == 0 else floor(riesgo_maximo / distancia_stop)
        )
        cantidad_por_exposicion = floor(exposicion_maxima / precio)
        cantidad = min(cantidad_por_riesgo, cantidad_por_exposicion)

        if cantidad < 1:
            return 0.0, 0.0, 0.0, "position_size_below_one_share"

        exposicion = cantidad * precio
        riesgo = cantidad * distancia_stop
        return float(cantidad), float(exposicion), float(riesgo), None

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
                if self._pnl_hoy() <= -self._limite_perdida_diaria():
                    return self._record(
                        signal_id, "blocked", "daily_loss_limit", now,
                        simbolo, precio, stop_loss
                    )
                return self._open(signal_id, simbolo, precio, stop_loss, signal, now)

            if accion == "EXIT":
                return self._close(signal_id, simbolo, precio, signal, now)

            if accion in {"HOLD", "WATCH", "WAIT"}:
                pos = self.positions.get(simbolo)
                if pos and stop_loss is not None:
                    if pos.stop_loss is None or stop_loss > pos.stop_loss:
                        pos.stop_loss = stop_loss
                return self._record(
                    signal_id, accion.lower(), str(signal.get("motivo") or accion.lower()), now,
                    simbolo, precio, pos.stop_loss if pos else stop_loss,
                    pos.position_id if pos else None,
                    pos.cantidad if pos else None,
                    pos.capital_expuesto if pos else None,
                    pos.riesgo_dolares if pos else None,
                )

            return self._record(
                signal_id, "ignored", f"unsupported_action:{accion}", now,
                simbolo, precio, stop_loss
            )

    def _open(self, signal_id, simbolo, precio, stop_loss, signal, now):
        if precio is None:
            return self._record(signal_id, "blocked", "BUY_without_price", now, simbolo, None, stop_loss)

        if simbolo in self.positions:
            p = self.positions[simbolo]
            return self._record(
                signal_id, "hold", "position_already_open", now,
                simbolo, precio, p.stop_loss, p.position_id,
                p.cantidad, p.capital_expuesto, p.riesgo_dolares
            )

        if len(self.positions) >= self.risk.max_simultaneous_positions:
            return self._record(
                signal_id, "blocked", "max_simultaneous_positions",
                now, simbolo, precio, stop_loss
            )

        cantidad, exposicion, riesgo, error = self._calcular_tamano(precio, stop_loss)
        if error:
            return self._record(signal_id, "blocked", error, now, simbolo, precio, stop_loss)

        position_id = uuid4().hex
        self.positions[simbolo] = PaperPosition(
            position_id, simbolo,
            str(signal.get("estrategia") or "PreMarketSalvajes LONG"),
            precio, stop_loss, cantidad, exposicion, riesgo, now
        )
        return self._record(
            signal_id, "buy", "paper_position_opened", now,
            simbolo, precio, stop_loss, position_id,
            cantidad, exposicion, riesgo
        )

    def _close(self, signal_id, simbolo, precio, signal, now):
        position = self.positions.pop(simbolo, None)
        if position is None:
            return self._record(
                signal_id, "ignored", "exit_without_open_position",
                now, simbolo, precio, signal.get("stop_loss")
            )

        pnl = None
        if precio is not None and position.cantidad is not None:
            pnl = round((precio - position.precio_entrada) * position.cantidad, 2)

        trade = {
            "position_id": position.position_id,
            "simbolo": simbolo,
            "estrategia": position.estrategia,
            "precio_entrada": position.precio_entrada,
            "precio_salida": precio,
            "cantidad": position.cantidad,
            "pnl_realizado": pnl if pnl is not None else 0.0,
            "fecha": self._fecha_operativa(),
            "closed_at": now,
            "motivo": str(signal.get("motivo") or "strategy_exit"),
        }
        self.closed_trades.append(trade)
        self.closed_trades = self.closed_trades[-1000:]

        return self._record(
            signal_id, "sell", trade["motivo"], now,
            simbolo, precio, position.stop_loss, position.position_id,
            position.cantidad, position.capital_expuesto,
            position.riesgo_dolares, pnl
        )

    def _record(
        self, signal_id, action, reason, now, simbolo="", precio=None,
        stop_loss=None, position_id=None, cantidad=None,
        capital_expuesto=None, riesgo_dolares=None, pnl_realizado=None
    ):
        item = asdict(Decision(
            uuid4().hex, signal_id, "paper", action, reason, now,
            simbolo, precio, stop_loss, position_id,
            cantidad, capital_expuesto, riesgo_dolares, pnl_realizado
        ))
        self.decisions.append(item)
        self.decisions = self.decisions[-1000:]
        return item

    def posiciones(self) -> list[dict]:
        with self._lock:
            return [asdict(p) for p in self.positions.values()]

    def operaciones_cerradas(self) -> list[dict]:
        with self._lock:
            return list(self.closed_trades)

    def status(self) -> dict:
        with self._lock:
            pnl_hoy = self._pnl_hoy()
            return {
                "modo": "paper",
                "capital_inicial": self.risk.initial_capital,
                "riesgo_maximo_por_operacion": self.risk.initial_capital * self.risk.max_risk_per_trade,
                "exposicion_maxima_por_posicion": self.risk.initial_capital * self.risk.max_exposure,
                "limite_perdida_diaria": self._limite_perdida_diaria(),
                "pnl_realizado_hoy": pnl_hoy,
                "perdida_diaria_disponible": max(0.0, self._limite_perdida_diaria() + pnl_hoy),
                "posiciones_abiertas": len(self.positions),
                "max_simultaneous_positions": self.risk.max_simultaneous_positions,
                "daily_loss_limit": self.risk.daily_loss_limit,
                "kill_switch": self.risk.kill_switch,
                "decisiones": len(self.decisions),
                "operaciones_cerradas": len(self.closed_trades),
            }
