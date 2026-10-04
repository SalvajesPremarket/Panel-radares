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


class MotorVelasBridge:
    """Conecta el motor de velas con los resultados del scanner sin decidir operaciones."""

    MAX_SIMBOLOS_BASIC = 30
    MAX_NUEVOS_POR_CICLO = 10

    def __init__(self, api_key: str, secret_key: str):
        from BotTradeScanner.motor_velas.motor_velas import MotorVelas

        self.motor = MotorVelas(api_key, secret_key)
        self._lock = Lock()
        self._subscribe_lock = Lock()
        self._arrancado = False
        self._hilo_inicio = None
        self._simbolos_solicitados = set()
        self._simbolos_cargados = set()
        self._ultima_error = None

        self._arrancar_stream()

    def _arrancar_stream(self):
        with self._lock:
            if self._arrancado:
                return
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

        self._hilo_inicio = Thread(
            target=_run,
            name="tradescanner-motor-velas",
            daemon=True,
        )
        self._hilo_inicio.start()

    def sync_results(self, resultados: Iterable[dict] | None):
        """Suscribe los candidatos actuales del scanner al motor de velas.

        Nunca modifica los resultados del scanner ni genera señales de compra/venta.
        """
        if not resultados:
            return

        candidatos = []
        for row in resultados:
            try:
                ticker = str(row.get("ticker", "")).strip().upper()
            except Exception:
                ticker = ""
            if ticker and ticker not in candidatos:
                candidatos.append(ticker)

        if not candidatos:
            return

        with self._lock:
            disponibles = [
                s for s in candidatos
                if s not in self._simbolos_solicitados
                and s not in self._simbolos_cargados
            ]
            capacidad = max(0, self.MAX_SIMBOLOS_BASIC - len(self._simbolos_solicitados))
            nuevos = disponibles[: min(self.MAX_NUEVOS_POR_CICLO, capacidad)]
            for symbol in nuevos:
                self._simbolos_solicitados.add(symbol)

        if not nuevos:
            return

        Thread(
            target=self._preparar_y_suscribir,
            args=(nuevos,),
            name="tradescanner-motor-velas-subscribe",
            daemon=True,
        ).start()

    def _preparar_y_suscribir(self, simbolos):
        # Solo una precarga/suscripción a la vez para no disparar llamadas
        # históricas concurrentes contra Alpaca.
        with self._subscribe_lock:
            try:
                # El motor existente intenta cargar historial de 1 minuto antes
                # de empezar a consumir trades. En el plan Basic, Alpaca limita
                # la ventana histórica disponible; se usa lo que el plan permita.
                self.motor.precargar_historial(simbolos, cantidad=300)

                for symbol in simbolos:
                    self.motor.agregar_simbolo_en_caliente(symbol)

                with self._lock:
                    self._simbolos_cargados.update(simbolos)
            except Exception as exc:
                self._ultima_error = str(exc)
                with self._lock:
                    for symbol in simbolos:
                        self._simbolos_solicitados.discard(symbol)

    def snapshot(self, simbolo: str) -> dict:
        return self.motor.snapshot_simbolo(str(simbolo).strip().upper())

    def status(self) -> dict:
        with self._lock:
            solicitados = sorted(self._simbolos_solicitados)
            cargados = sorted(self._simbolos_cargados)

        hilo = self._hilo_inicio
        vivo = bool(hilo is not None and hilo.is_alive())

        return {
            "stream_hilo_vivo": vivo,
            "stream_iniciado": bool(getattr(self.motor, "_iniciado", False)),
            "simbolos_solicitados": solicitados,
            "simbolos_cargados": cargados,
            "total_trades": int(getattr(self.motor, "total_trades", 0) or 0),
            "ultimo_trade": getattr(self.motor, "ultimo_trade", None),
            "error": self._ultima_error,
            "limite_simbolos": self.MAX_SIMBOLOS_BASIC,
        }
