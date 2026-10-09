"""Puente entre ServicioScanner y MotorVelas.

No toma decisiones de trading. Su responsabilidad es:
1) arrancar una única conexión de market-data websocket;
2) recibir los candidatos publicados por el scanner;
3) suscribir hasta 30 símbolos (límite de Alpaca Basic);
4) precargar el historial disponible antes de empezar a observarlos en vivo;
5) exponer snapshots del motor para que el bot real pueda consumirlos posteriormente.

El bot de estrategia NO vive aquí.
"""

from __future__ import annotations

from threading import Lock, Thread
from typing import Iterable
import time


class MotorVelasBridge:
    """Conecta el motor de velas con los resultados del scanner sin decidir operaciones."""

    # Safety margin under the Basic plan limit; each ticker uses trades + quotes.
    MAX_SIMBOLOS_BASIC = 10
    MAX_NUEVOS_POR_CICLO = 10

    def __init__(self, api_key: str, secret_key: str, motor=None, market_stream=None):
        if motor is None:
            from BotTradeScanner.motor_velas.motor_velas import MotorVelas
            motor = MotorVelas(api_key, secret_key)

        self.motor = motor
        self.market_stream = market_stream
        if self.market_stream is not None:
            # Compatibilidad defensiva: Cloud puede conservar una instancia
            # antigua del módulo durante un hot-reload. Si la interfaz pública
            # no está presente, no abortamos el scanner al importar el bridge.
            if not hasattr(self.market_stream, "add_consumer"):
                consumidores_t = getattr(self.market_stream, "_trade_consumers", None)
                consumidores_q = getattr(self.market_stream, "_quote_consumers", None)
                if isinstance(consumidores_t, list) and self.motor._recibir_trade_compartido not in consumidores_t:
                    consumidores_t.append(self.motor._recibir_trade_compartido)
                if isinstance(consumidores_q, list) and self.motor._recibir_quote_compartido not in consumidores_q:
                    consumidores_q.append(self.motor._recibir_quote_compartido)
            self.motor.conectar_stream_compartido(self.market_stream)
        self._lock = Lock()
        self._subscribe_lock = Lock()
        self._arrancado = False
        self._hilo_inicio = None
        self._simbolos_solicitados = set()
        self._simbolos_cargados = set()
        self._simbolos_deseados = set()
        self._ultima_error = None
        self._proximo_reintento_ts = 0.0
        self._cooldown_reconexion_seg = 120.0

        # El websocket es LAZY: no se abre mientras no existan candidatos.

    def _arrancar_stream(self):
        if getattr(self, "market_stream", None) is not None:
            self._arrancado = True
            return True
        with self._lock:
            if self._arrancado:
                return True
            ahora = time.monotonic()
            if ahora < self._proximo_reintento_ts:
                return False
            self._arrancado = True

        def _run():
            try:
                # Arrancamos el websocket sin suscribir todo el universo.
                # Los candidatos se agregan dinámicamente mediante sync_results().
                self.motor.iniciar([])
            except Exception as exc:
                self._ultima_error = str(exc)
                with self._lock:
                    self._arrancado = False
                    self._proximo_reintento_ts = time.monotonic() + self._cooldown_reconexion_seg

        self._hilo_inicio = Thread(
            target=_run,
            name="tradescanner-motor-velas",
            daemon=True,
        )
        self._hilo_inicio.start()

    def sync_results(self, resultados: Iterable[dict] | None):
        """Ajusta la suscripcion al conjunto actual de candidatos."""
        candidatos = []
        for row in resultados or []:
            try:
                ticker = str(row.get("ticker", "")).strip().upper()
            except Exception:
                ticker = ""
            if ticker and ticker not in candidatos:
                candidatos.append(ticker)

        deseados = set(candidatos)
        # Always synchronize, even for an empty list, so stale candidates are
        # removed from the shared stream when scanner results disappear.
        if getattr(self, "market_stream", None) is not None:
            try:
                self.market_stream.start(candidatos[: self.MAX_SIMBOLOS_BASIC])
            except Exception as exc:
                self._ultima_error = str(exc)
        if deseados:
            self._arrancar_stream()
        with self._lock:
            self._simbolos_deseados = deseados

            # Simbolos ya suscritos que salieron del conjunto actual.
            retirar = [
                s for s in self._simbolos_cargados
                if s not in deseados
            ]
            for symbol in retirar:
                self._simbolos_cargados.discard(symbol)

            # Si estaban en preparacion pero dejaron de ser necesarios,
            # el worker no los suscribira cuando termine la precarga.
            self._simbolos_solicitados.difference_update(retirar)

            disponibles = [
                s for s in candidatos
                if s not in self._simbolos_solicitados
                and s not in self._simbolos_cargados
            ]
            ocupados = len(self._simbolos_solicitados | self._simbolos_cargados)
            capacidad = max(0, self.MAX_SIMBOLOS_BASIC - ocupados)
            nuevos = disponibles[: min(self.MAX_NUEVOS_POR_CICLO, capacidad)]
            for symbol in nuevos:
                self._simbolos_solicitados.add(symbol)

        if retirar or nuevos:
            Thread(
                target=self._actualizar_suscripciones,
                args=(retirar, nuevos),
                name="tradescanner-motor-velas-subscribe",
                daemon=True,
            ).start()

    def _esperar_stream(self, timeout=15.0):
        limite_espera = time.monotonic() + timeout
        while getattr(self.motor, "_stream", None) is None:
            if time.monotonic() >= limite_espera:
                raise RuntimeError("stream_market_data_no_disponible")
            time.sleep(0.1)

    def _actualizar_suscripciones(self, retirar, nuevos):
        # Solo una precarga/suscripcion a la vez para no disparar llamadas
        # historicas concurrentes contra Alpaca.
        with self._subscribe_lock:
            try:
                if getattr(self, "market_stream", None) is None:
                    self._esperar_stream()

                for symbol in retirar:
                    if getattr(self, "market_stream", None) is None:
                        self.motor.quitar_simbolo_en_caliente(symbol)
                    with self._lock:
                        self._simbolos_cargados.discard(symbol)

                if nuevos:
                    self.motor.precargar_historial(nuevos, cantidad=300)

                for symbol in nuevos:
                    with self._lock:
                        sigue_deseado = symbol in self._simbolos_deseados
                    if not sigue_deseado:
                        with self._lock:
                            self._simbolos_solicitados.discard(symbol)
                        continue
                    if getattr(self, "market_stream", None) is None:
                        self.motor.agregar_simbolo_en_caliente(symbol)
                    with self._lock:
                        self._simbolos_solicitados.discard(symbol)
                        self._simbolos_cargados.add(symbol)
            except Exception as exc:
                self._ultima_error = str(exc)
                with self._lock:
                    for symbol in nuevos:
                        self._simbolos_solicitados.discard(symbol)

    def snapshot(self, simbolo: str) -> dict:
        return self.motor.snapshot_simbolo(str(simbolo).strip().upper())

    def status(self) -> dict:
        with self._lock:
            solicitados = sorted(self._simbolos_solicitados)
            cargados = sorted(self._simbolos_cargados)

        # En modo compartido, este bridge NO crea su propio hilo de websocket.
        # El websocket real vive en AlpacaMarketStream del scanner.
        stream_compartido = getattr(self, "market_stream", None)
        if stream_compartido is not None:
            try:
                salud = stream_compartido.health_snapshot()
            except Exception:
                salud = {}
            vivo = bool(salud.get("running"))
            conectado = bool(salud.get("connected"))
            error_stream = str(salud.get("last_error") or "")
        else:
            hilo = self._hilo_inicio
            vivo = bool(hilo is not None and hilo.is_alive())
            conectado = bool(getattr(self.motor, "_iniciado", False))
            error_stream = ""

        return {
            "stream_hilo_vivo": vivo,
            "stream_iniciado": conectado,
            "simbolos_solicitados": solicitados,
            "simbolos_cargados": cargados,
            "total_trades": int(getattr(self.motor, "total_trades", 0) or 0),
            "stream_trades": int(salud.get("trades", 0) or 0) if stream_compartido is not None else int(getattr(self.motor, "total_trades", 0) or 0),
            "consumer_errors": int(salud.get("consumer_errors", 0) or 0) if stream_compartido is not None else 0,
            "trade_consumer_errors": int(salud.get("trade_consumer_errors", 0) or 0) if stream_compartido is not None else 0,
            "quote_consumer_errors": int(salud.get("quote_consumer_errors", 0) or 0) if stream_compartido is not None else 0,
            "last_consumer_error": str(salud.get("last_consumer_error") or "") if stream_compartido is not None else "",
            "ultimo_trade": getattr(self.motor, "ultimo_trade", None),
            "error": self._ultima_error or error_stream,
            "limite_simbolos": self.MAX_SIMBOLOS_BASIC,
            "stream_compartido": stream_compartido is not None,
            "feed": str(salud.get("feed") or "") if stream_compartido is not None else "",
            "stream_last_error": str(salud.get("last_error") or "") if stream_compartido is not None else error_stream,
            "stream_running": bool(salud.get("running")) if stream_compartido is not None else vivo,
            "stream_connected": bool(salud.get("connected")) if stream_compartido is not None else conectado,
            "symbols_seen": int(salud.get("symbols_seen", 0) or 0) if stream_compartido is not None else 0,
            "stream_errors": int(salud.get("errors", 0) or 0) if stream_compartido is not None else 0,
            "quotes": int(salud.get("quotes", 0) or 0) if stream_compartido is not None else 0,
            "last_event_kind": str(salud.get("last_event_kind") or "") if stream_compartido is not None else "",
            "last_event_symbol": str(salud.get("last_event_symbol") or "") if stream_compartido is not None else "",
            "last_event_age_sec": salud.get("last_event_age_sec") if stream_compartido is not None else None,
            "subscribed_symbols": list(salud.get("subscribed_symbols") or []) if stream_compartido is not None else [],
            "last_subscription_request_ts": salud.get("last_subscription_request_ts") if stream_compartido is not None else None,
        }
