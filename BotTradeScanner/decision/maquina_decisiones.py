"""Maquina de decisiones del BotTradeScanner.

Esta capa coordina candidatos del TradeScanner con la estrategia LONG.
No contiene Streamlit, no contiene credenciales y no envia ordenes.

Por ahora solo existe LONG PreMarketSalvajes.
SHORT queda fuera deliberadamente.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from threading import Lock
from typing import Any, Iterable

from BotTradeScanner.estrategias.long.premarket_salvajes import (
    DecisionLong,
    EstadoLong,
    PreMarketSalvajesLong,
)


@dataclass(frozen=True)
class DecisionBot:
    simbolo: str
    accion: str
    estado: str
    motivo: str
    stop_loss: float | None = None
    precio: float | None = None
    candidato_scanner: bool = False
    estrategia: str = "PreMarketSalvajes LONG"


class MaquinaDecisionesLong:
    """Una estrategia LONG independiente por simbolo observado."""

    def __init__(self) -> None:
        self._estrategias: dict[str, PreMarketSalvajesLong] = {}
        self._lock = Lock()

    def _estrategia(self, simbolo: str) -> PreMarketSalvajesLong:
        simbolo = str(simbolo).strip().upper()
        if simbolo not in self._estrategias:
            self._estrategias[simbolo] = PreMarketSalvajesLong()
        return self._estrategias[simbolo]

    @staticmethod
    def _precio_actual(snap: dict[str, Any]) -> float | None:
        vela = snap.get("vela_actual") or {}
        value = vela.get("cierre")
        return float(value) if value is not None else None

    def evaluar(
        self,
        snap: dict[str, Any],
        candidato_scanner: bool = True,
    ) -> DecisionBot:
        simbolo = str(snap.get("simbolo", "")).strip().upper()
        if not simbolo:
            return DecisionBot("", "WAIT", "sin_simbolo", "Snapshot sin simbolo.")

        with self._lock:
            estrategia = self._estrategia(simbolo)

            # El stop es gestion operacional de la posicion, no un porcentaje
            # inventado. La estrategia determina el nivel y esta maquina solo
            # detecta si el precio ya lo alcanzo.
            precio = self._precio_actual(snap)
            if (
                estrategia.estado
                in {
                    EstadoLong.LONG_PRIMERA_VELA,
                    EstadoLong.LONG_SEGUNDA_VELA,
                    EstadoLong.LONG_SIGUIENTES,
                }
                and estrategia.stop_loss is not None
                and precio is not None
                and precio <= estrategia.stop_loss
            ):
                decision = DecisionLong(
                    "EXIT",
                    estrategia.estado,
                    "El precio alcanzo el stop_loss vigente.",
                    stop_loss=estrategia.stop_loss,
                )
            elif not candidato_scanner and estrategia.estado == EstadoLong.ESPERANDO_CANDIDATO:
                decision = DecisionLong(
                    "WAIT",
                    estrategia.estado,
                    "El simbolo no fue entregado como candidato por TradeScanner.",
                )
            else:
                decision = estrategia.evaluar(snap)

            return DecisionBot(
                simbolo=simbolo,
                accion=decision.accion,
                estado=decision.estado.value,
                motivo=decision.motivo,
                stop_loss=decision.stop_loss,
                precio=precio,
                candidato_scanner=candidato_scanner,
            )

    def marcar_salida_para_pullback(self, simbolo: str) -> None:
        with self._lock:
            self._estrategia(simbolo).marcar_salida_para_pullback()

    def estado(self, simbolo: str) -> dict[str, Any]:
        with self._lock:
            estrategia = self._estrategia(simbolo)
            return {
                "simbolo": simbolo.upper(),
                "estado": estrategia.estado.value,
                "precio_entrada": estrategia.precio_entrada,
                "stop_loss": estrategia.stop_loss,
                "estrategia": "PreMarketSalvajes LONG",
            }

    def simbolos(self) -> list[str]:
        with self._lock:
            return sorted(self._estrategias.keys())


class BotTradeScannerLong:
    """Fachada pequeña para integrar la maquina con otros componentes."""

    def __init__(self) -> None:
        self.decisiones = MaquinaDecisionesLong()

    def procesar_snapshot(
        self,
        snapshot: dict[str, Any],
        candidato_scanner: bool = True,
    ) -> dict[str, Any]:
        return asdict(self.decisiones.evaluar(snapshot, candidato_scanner))

    def procesar_candidatos(
        self,
        snapshots: Iterable[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return [
            self.procesar_snapshot(snapshot, candidato_scanner=True)
            for snapshot in snapshots
        ]
