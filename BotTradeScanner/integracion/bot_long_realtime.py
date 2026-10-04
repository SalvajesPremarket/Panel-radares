"""Motor de señales LONG en tiempo real.

Conecta:
    candidatos de TradeScanner -> MotorVelasBridge -> MaquinaDecisionesLong

Esta pieza NO envia ordenes a Alpaca.
Su funcion en esta fase es evaluar continuamente los snapshots del motor,
mantener la maquina de estados por simbolo y conservar las decisiones para
la siguiente capa de riesgo/ejecucion.

La frecuencia de evaluacion es independiente del refresh visual del scanner.
"""
from __future__ import annotations

from collections import deque
from threading import Event, Lock, Thread
import time
from typing import Iterable

from BotTradeScanner.decision.maquina_decisiones import MaquinaDecisionesLong
from BotTradeScanner.riesgo.paper import PaperBot


class BotLongRealtime:
    """Orquestador del bot LONG sin ejecucion real de ordenes."""

    def __init__(self, motor_bridge, intervalo_segundos: float = 1.0, max_decisiones: int = 1000):
        self.motor_bridge = motor_bridge
        self.intervalo_segundos = max(0.2, float(intervalo_segundos))
        self.max_decisiones = max(100, int(max_decisiones))

        self.decisiones = MaquinaDecisionesLong()
        self.paper = PaperBot()
        self._candidatos: set[str] = set()
        self._lock = Lock()
        self._detener = Event()
        self._hilo: Thread | None = None
        self._decisiones = deque(maxlen=self.max_decisiones)
        self._ultima_decision_por_simbolo: dict[str, dict] = {}
        self._ultimo_error: str | None = None
        self._ciclos = 0
        self._ultima_evaluacion = None

    def iniciar(self) -> None:
        with self._lock:
            if self._hilo is not None and self._hilo.is_alive():
                return
            self._detener.clear()
            self._hilo = Thread(
                target=self._bucle,
                name="bot-long-realtime",
                daemon=True,
            )
            self._hilo.start()

    def detener(self) -> None:
        self._detener.set()

    def sync_candidates(self, resultados: Iterable[dict] | None) -> None:
        """Actualiza los candidatos sin borrar posiciones que ya estén activas."""
        candidatos: set[str] = set()
        for row in resultados or []:
            try:
                ticker = str(row.get("ticker", "")).strip().upper()
            except Exception:
                ticker = ""
            if ticker:
                candidatos.add(ticker)

        with self._lock:
            self._candidatos = candidatos

        # La suscripcion al market-data sigue siendo responsabilidad del bridge.
        self.motor_bridge.sync_results(resultados)

    def evaluar_ahora(self) -> list[dict]:
        with self._lock:
            candidatos = set(self._candidatos)

        simbolos = set(candidatos)
        # Una posicion activa debe seguir gestionandose aunque el scanner ya
        # no publique el ticker como candidato en el siguiente ciclo.
        for simbolo in self.decisiones.simbolos():
            estado = self.decisiones.estado(simbolo).get("estado", "")
            if estado in {
                "long_primera_vela",
                "long_segunda_vela",
                "long_siguientes",
            }:
                simbolos.add(simbolo)

        nuevas = []
        for simbolo in sorted(simbolos):
            try:
                snap = self.motor_bridge.snapshot(simbolo)
                candidato = simbolo in candidatos
                decision = self.decisiones.procesar_snapshot(snap, candidato_scanner=candidato)

                # No guardamos WAIT repetitivos sin cambio para no llenar la cola.
                anterior = self._ultima_decision_por_simbolo.get(simbolo)
                comparable = {
                    "accion": decision.get("accion"),
                    "estado": decision.get("estado"),
                    "motivo": decision.get("motivo"),
                    "stop_loss": decision.get("stop_loss"),
                }
                if anterior != comparable or decision.get("accion") in {"BUY", "EXIT"}:
                    paper = self.paper.evaluar(decision)
                    registro = {
                        **decision,
                        "paper": paper,
                        "ts": time.time(),
                    }
                    with self._lock:
                        self._decisiones.append(registro)
                        self._ultima_decision_por_simbolo[simbolo] = comparable
                    nuevas.append(registro)
            except Exception as exc:
                self._ultimo_error = f"{simbolo}: {exc}"

        self._ciclos += 1
        self._ultima_evaluacion = time.time()
        return nuevas

    def _bucle(self) -> None:
        while not self._detener.is_set():
            inicio = time.monotonic()
            try:
                self.evaluar_ahora()
            except Exception as exc:
                self._ultimo_error = str(exc)

            restante = self.intervalo_segundos - (time.monotonic() - inicio)
            if restante > 0:
                self._detener.wait(restante)

    def decisiones_recientes(self, limite: int = 100) -> list[dict]:
        with self._lock:
            datos = list(self._decisiones)
        return datos[-max(1, int(limite)):]

    def status(self) -> dict:
        with self._lock:
            candidatos = sorted(self._candidatos)
            hilo = self._hilo

        return {
            "hilo_vivo": bool(hilo is not None and hilo.is_alive()),
            "intervalo_segundos": self.intervalo_segundos,
            "candidatos": candidatos,
            "ciclos": self._ciclos,
            "ultima_evaluacion": self._ultima_evaluacion,
            "ultimo_error": self._ultimo_error,
            "decisiones_guardadas": len(self._decisiones),
            "paper": self.paper.status(),
            "posiciones_paper": self.paper.posiciones(),
            "posiciones_observadas": [
                self.decisiones.estado(s)["estado"]
                for s in self.decisiones.simbolos()
                if self.decisiones.estado(s)["estado"]
                in {"long_primera_vela", "long_segunda_vela", "long_siguientes"}
            ],
        }
