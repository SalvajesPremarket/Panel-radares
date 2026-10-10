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

    # Basic allows 30 websocket channels. A trade subscription also exposes
    # corrections and cancelErrors, plus quotes: cap at 7 tickers (28 channels)
    # to stay below the limit with room for protocol/accounting differences.
    MAX_SIMBOLOS_BASIC = 7
    MAX_NUEVOS_POR_CICLO = 7

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
        self._simbolos_deseados_ordenados = []
        self._ultima_error = None
        self._estado_preparacion = "Esperando candidatos"
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

        # Enfocar el bridge en los mismos símbolos que realmente puede recibir
        # el websocket. También limpia motores precargados por una versión
        # anterior que permitía más símbolos.
        seleccionados = candidatos[: self.MAX_SIMBOLOS_BASIC]
        deseados = set(seleccionados)
        # If no candidates remain, release their subscriptions immediately.
        # For new candidates, defer stream subscription until historical bars
        # are loaded; otherwise live trades can race with cargar_historial().
        stream_compartido = getattr(self, "market_stream", None) is not None
        if stream_compartido and not seleccionados:
            try:
                self.market_stream.start([])
            except Exception as exc:
                self._ultima_error = str(exc)
        if deseados:
            self._arrancar_stream()
        with self._lock:
            self._simbolos_deseados = deseados
            self._simbolos_deseados_ordenados = list(seleccionados)

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
                s for s in seleccionados
                if s not in self._simbolos_solicitados
                and s not in self._simbolos_cargados
            ]
            ocupados = len(self._simbolos_solicitados | self._simbolos_cargados)
            capacidad = max(0, self.MAX_SIMBOLOS_BASIC - ocupados)
            nuevos = disponibles[: min(self.MAX_NUEVOS_POR_CICLO, capacidad)]
            for symbol in nuevos:
                self._simbolos_solicitados.add(symbol)

        if retirar or nuevos:
            if nuevos:
                with self._lock:
                    self._estado_preparacion = f"Preparando historial: {\", \".join(nuevos)}"
            elif retirar:
                with self._lock:
                    self._estado_preparacion = "Retirando símbolos anteriores"
            if stream_compartido:
                # Keep only already-prepared desired symbols while the worker
                # loads history for new candidates. This unsubscribes retired
                # tickers before their local engines are removed, without
                # exposing new symbols to live trades before their history exists.
                with self._lock:
                    preparados_existentes = [
                        s for s in self._simbolos_deseados_ordenados
                        if s in self._simbolos_cargados
                    ]
                try:
                    self.market_stream.start(preparados_existentes)
                except Exception as exc:
                    self._ultima_error = str(exc)
            Thread(
                target=self._actualizar_suscripciones,
                args=(retirar, nuevos),
                name="tradescanner-motor-velas-subscribe",
                daemon=True,
            ).start()
        elif stream_compartido and seleccionados:
            with self._lock:
                preparados = all(s in self._simbolos_cargados for s in seleccionados)
            if preparados:
                try:
                    self.market_stream.start(seleccionados)
                except Exception as exc:
                    self._ultima_error = str(exc)

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
            etapa = "esperando el stream del motor"
            try:
                if getattr(self, "market_stream", None) is None:
                    with self._lock:
                        self._estado_preparacion = etapa
                    self._esperar_stream()

                for symbol in retirar:
                    # El websocket compartido ya actualizó sus suscripciones;
                    # ahora liberar también el motor/caché local del ticker viejo.
                    self.motor.quitar_simbolo_en_caliente(symbol)
                    with self._lock:
                        self._simbolos_cargados.discard(symbol)

                if nuevos:
                    etapa = f"precargando historial para {\", \".join(nuevos)}"
                    with self._lock:
                        self._estado_preparacion = etapa
                    self.motor.precargar_historial(nuevos, cantidad=300)

                for symbol in nuevos:
                    etapa = f"registrando símbolo {symbol}"
                    with self._lock:
                        self._estado_preparacion = etapa
                        sigue_deseado = symbol in self._simbolos_deseados
                    if not sigue_deseado:
                        with self._lock:
                            self._simbolos_solicitados.discard(symbol)
                            self._simbolos_cargados.discard(symbol)
                        # History may already have created an engine for a
                        # candidate that disappeared during the API request.
                        # It was never subscribed, so safely release its cache.
                        self.motor.quitar_simbolo_en_caliente(symbol)
                        continue
                    if getattr(self, "market_stream", None) is None:
                        self.motor.agregar_simbolo_en_caliente(symbol)
                    with self._lock:
                        self._simbolos_solicitados.discard(symbol)
                        self._simbolos_cargados.add(symbol)

                if getattr(self, "market_stream", None) is not None:
                    with self._lock:
                        desired_order = list(self._simbolos_deseados_ordenados)
                        prepared = all(s in self._simbolos_cargados for s in desired_order)
                    if prepared:
                        etapa = f"solicitando suscripción WebSocket: {\", \".join(desired_order)}"
                        with self._lock:
                            self._estado_preparacion = etapa
                        self.market_stream.start(desired_order)
                with self._lock:
                    self._ultima_error = None
                    self._estado_preparacion = "Historial preparado; suscripción solicitada" if desired_order else "Esperando candidatos"
            except Exception as exc:
                self._ultima_error = f"{etapa}: {type(exc).__name__}: {exc}"[:500]
                with self._lock:
                    self._estado_preparacion = f"ERROR en {etapa}"
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
            "estado_preparacion": self._estado_preparacion,
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
            "server_subscription_state": dict(salud.get("server_subscription_state") or {}) if stream_compartido is not None else {},
            "last_subscription_ack_ts": salud.get("last_subscription_ack_ts") if stream_compartido is not None else None,
        }
