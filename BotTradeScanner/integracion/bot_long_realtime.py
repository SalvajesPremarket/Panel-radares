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
from dataclasses import asdict
from threading import Event, Lock, Thread
import time
from typing import Iterable

from BotTradeScanner.decision.maquina_decisiones import MaquinaDecisionesLong
from BotTradeScanner.riesgo.paper import PaperBot
from BotTradeScanner.ejecucion.configuracion import ExecutionConfig
from BotTradeScanner.ejecucion.alpaca import AlpacaExecutor, preparar_buy, ESTADOS_TERMINALES


class BotLongRealtime:
    """Orquestador del bot LONG sin ejecucion real de ordenes."""

    def __init__(self, motor_bridge, intervalo_segundos: float = 1.0, max_decisiones: int = 1000, execution_config: ExecutionConfig | None = None, executor: AlpacaExecutor | None = None):
        self.motor_bridge = motor_bridge
        self.intervalo_segundos = max(0.2, float(intervalo_segundos))
        self.max_decisiones = max(100, int(max_decisiones))

        self.decisiones = MaquinaDecisionesLong()
        self.paper = PaperBot()
        self.execution_config = execution_config or ExecutionConfig.por_defecto()
        self.execution_config.validar()
        self.executor = executor
        self._ordenes_pendientes: dict[str, dict] = {}
        self._candidatos: set[str] = set()
        self._candidate_metadata: dict[str, dict] = {}
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
        """Actualiza candidatos desde las filas del scanner sin alterar la estrategia."""
        self.sync_signals(resultados)

    def sync_signals(self, signals: Iterable[dict] | None) -> None:
        """Acepta filas del scanner o señales normalizadas del API.

        Solo convierte la identidad de la señal en un candidato de mercado.
        No convierte LONG en BUY: la entrada sigue dependiendo de MotorVelas
        + MaquinaDecisionesLong + PreMarketSalvajes.
        """
        candidatos: set[str] = set()
        metadata: dict[str, dict] = {}
        for row in signals or []:
            try:
                ticker = str(row.get("ticker") or row.get("symbol") or row.get("simbolo") or "").strip().upper()
            except Exception:
                ticker = ""
            if ticker:
                candidatos.add(ticker)
                metadata[ticker] = {
                    "signal_id": row.get("signal_id") or row.get("signalId"),
                    "confidence": row.get("confidence"),
                    "signal_type": row.get("signal_type") or row.get("signalType"),
                    "timeframe": row.get("tecnico_timeframe") or row.get("timeframe"),
                }

        with self._lock:
            self._candidatos = candidatos
            self._candidate_metadata = metadata

        activos = set()
        for simbolo in self.decisiones.simbolos():
            estado = self.decisiones.estado(simbolo).get("estado", "")
            if estado in {
                "long_primera_vela",
                "long_segunda_vela",
                "long_siguientes",
            }:
                activos.add(simbolo)

        observados = candidatos | activos
        self.motor_bridge.sync_results([{"ticker": s} for s in sorted(observados)])

    def _procesar_ordenes_pendientes(self) -> tuple[list[dict], set[str]]:
        """Consulta fills/rechazos antes de evaluar nuevas entradas."""
        novedades = []
        fills_confirmados: set[str] = set()
        for simbolo, info in list(self._ordenes_pendientes.items()):
            cid = info["client_order_id"]
            try:
                result = self.executor.consultar(cid) if self.executor else None
                if result is None:
                    continue
                if result.status == "filled" and result.filled_avg_price is not None and result.filled_qty > 0:
                    self.decisiones.confirmar_fill(simbolo, result.filled_avg_price)
                    estado = self.decisiones.estado(simbolo)
                    signal = {
                        "simbolo": simbolo,
                        "accion": "BUY",
                        "estado": estado["estado"],
                        "motivo": "BUY confirmado por FILLED Alpaca Paper",
                        "precio": result.filled_avg_price,
                        "cantidad_ejecutada": result.filled_qty,
                        "stop_loss": estado["stop_loss"],
                        "estrategia": "PreMarketSalvajes LONG",
                        "signal_id": info.get("signal_id") or cid,
                        "confidence": info.get("confidence"),
                        "signal_type": info.get("signal_type"),
                        "timeframe": info.get("timeframe"),
                    }
                    paper = self.paper.evaluar(signal)
                    novedades.append({**signal, "ejecucion": asdict(result), "paper": paper, "ts": time.time()})
                    fills_confirmados.add(simbolo)
                    del self._ordenes_pendientes[simbolo]
                elif result.status in ESTADOS_TERMINALES and result.status != "filled":
                    self.decisiones.cancelar_entrada_pendiente(simbolo)
                    novedades.append({
                        "simbolo": simbolo,
                        "accion": "WAIT",
                        "estado": "esperando_libelula",
                        "motivo": f"orden_{result.status}",
                        "signal_id": info.get("signal_id"),
                        "confidence": info.get("confidence"),
                        "signal_type": info.get("signal_type"),
                        "timeframe": info.get("timeframe"),
                        "ejecucion": asdict(result),
                        "ts": time.time(),
                    })
                    del self._ordenes_pendientes[simbolo]
            except Exception as exc:
                self._ultimo_error = f"{simbolo}: error consultando orden: {exc}"
        return novedades, fills_confirmados

    def evaluar_ahora(self) -> list[dict]:
        with self._lock:
            candidatos = set(self._candidatos)
            candidate_metadata = dict(self._candidate_metadata)

        simbolos = set(candidatos)
        for simbolo in self.decisiones.simbolos():
            estado = self.decisiones.estado(simbolo).get("estado", "")
            if estado in {
                "long_primera_vela",
                "long_segunda_vela",
                "long_siguientes",
            }:
                simbolos.add(simbolo)

        nuevas, fills_confirmados = self._procesar_ordenes_pendientes()
        for simbolo in sorted(simbolos):
            if simbolo in fills_confirmados:
                continue
            try:
                snap = self.motor_bridge.snapshot(simbolo)
                candidato = simbolo in candidatos
                decision = self.decisiones.evaluar(snap, candidato_scanner=candidato)
                decision_data = asdict(decision)
                signal_meta = candidate_metadata.get(simbolo, {})

                # La identidad/confianza de la señal acompaña la decisión,
                # pero NO modifica las reglas de entrada de la estrategia.
                decision_data["signal_id"] = signal_meta.get("signal_id")
                decision_data["confidence"] = signal_meta.get("confidence")
                decision_data["signal_type"] = signal_meta.get("signal_type")
                decision_data["timeframe"] = signal_meta.get("timeframe")

                anterior = self._ultima_decision_por_simbolo.get(simbolo)
                comparable = {
                    "accion": decision_data.get("accion"),
                    "estado": decision_data.get("estado"),
                    "motivo": decision_data.get("motivo"),
                    "stop_loss": decision_data.get("stop_loss"),
                }
                if anterior != comparable or decision_data.get("accion") in {"BUY", "EXIT"}:
                    ejecucion = None
                    if decision_data.get("accion") == "BUY":
                        if self.executor is not None:
                            ask = snap.get("ask")
                            bid = snap.get("bid")
                            try:
                                precio_orden = ask if ask is not None else bid
                                stop_para_riesgo = decision_data.get("stop_loss")
                                if precio_orden is None:
                                    raise ValueError("quote_sin_precio")
                                cantidad, _, _, error_tamano = self.paper.validar_entrada(precio_orden, stop_para_riesgo, posiciones_reservadas=len(self._ordenes_pendientes))
                                if error_tamano:
                                    raise ValueError(error_tamano)
                                orden = self.executor.preparar(
                                    simbolo, ask=ask, bid=bid, cantidad=cantidad,
                                    mercado="regular",
                                    client_order_id=f"paper-{simbolo.lower()}-{int(time.time() * 1000)}",
                                    sesion=self.execution_config.regular,
                                )
                                resultado_envio = self.executor.enviar_buy(orden)
                                ejecucion = asdict(resultado_envio)
                                if resultado_envio.enviada and resultado_envio.status not in ESTADOS_TERMINALES:
                                    self._ordenes_pendientes[simbolo] = {
                                        "client_order_id": orden.client_order_id,
                                        "simbolo": simbolo,
                                        "signal_id": decision_data.get("signal_id"),
                                        "confidence": decision_data.get("confidence"),
                                        "signal_type": decision_data.get("signal_type"),
                                        "timeframe": decision_data.get("timeframe"),
                                    }
                                    decision_data["accion"] = "WAIT"
                                    decision_data["motivo"] = "buy_order_submitted_waiting_fill"
                                elif resultado_envio.status == "filled" and resultado_envio.filled_avg_price is not None:
                                    self.decisiones.confirmar_fill(simbolo, resultado_envio.filled_avg_price)
                                    decision_data["precio"] = resultado_envio.filled_avg_price
                                    decision_data["cantidad_ejecutada"] = resultado_envio.filled_qty
                                else:
                                    self.decisiones.cancelar_entrada_pendiente(simbolo)
                                    decision_data["accion"] = "WAIT"
                                    decision_data["motivo"] = f"execution_{resultado_envio.status}"
                            except (ValueError, TypeError) as exc:
                                self.decisiones.cancelar_entrada_pendiente(simbolo)
                                ejecucion = {"bloqueado": str(exc)}
                                decision_data["accion"] = "WAIT"
                                decision_data["motivo"] = f"execution_blocked:{exc}"
                        else:
                            ask = snap.get("ask")
                            bid = snap.get("bid")
                            try:
                                precio_orden = ask if ask is not None else bid
                                stop_para_riesgo = decision_data.get("stop_loss")
                                if precio_orden is None:
                                    raise ValueError("quote_sin_precio")
                                cantidad, _, _, error_tamano = self.paper.tamano_entrada(precio_orden, stop_para_riesgo)
                                if error_tamano:
                                    raise ValueError(error_tamano)
                                orden = preparar_buy(
                                    simbolo,
                                    ask=ask,
                                    bid=bid,
                                    cantidad=cantidad,
                                    sesion=self.execution_config.regular,
                                    mercado="regular",
                                    client_order_id=f"paper-{simbolo.lower()}-{int(time.time() * 1000)}",
                                )
                                ejecucion = {
                                    "tipo": orden.tipo,
                                    "limit_price": orden.limit_price,
                                    "extended_hours": orden.extended_hours,
                                    "time_in_force": orden.time_in_force,
                                    "client_order_id": orden.client_order_id,
                                }
                                decision_data["precio"] = orden.limit_price
                                self.decisiones.confirmar_fill(simbolo, orden.limit_price)
                            except (ValueError, TypeError) as exc:
                                ejecucion = {"bloqueado": str(exc)}
                                decision_data["accion"] = "WAIT"
                                decision_data["motivo"] = f"execution_blocked:{exc}"

                    paper = self.paper.evaluar(decision_data)
                    registro = {
                        **decision_data,
                        "ejecucion": ejecucion,
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
