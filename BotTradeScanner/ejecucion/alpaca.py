"""Capa segura de preparacion y envio de ordenes Alpaca.

La estrategia decide BUY/EXIT; esta capa decide COMO ejecutar.
Por defecto no envia ordenes (ExecutionConfig.enabled=False).
Acepta un TradingClient inyectado para pruebas o para una fase posterior.

La proteccion contra duplicados usa client_order_id, que Alpaca permite
consultar de forma determinista.
"""
from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any

from BotTradeScanner.ejecucion.configuracion import precio_limit


ESTADOS_TERMINALES = {"filled", "canceled", "expired", "rejected", "done_for_day"}
ESTADOS_ACTIVOS = {
    "new",
    "accepted",
    "pending_new",
    "partially_filled",
    "pending_cancel",
    "pending_replace",
}


@dataclass(frozen=True)
class OrdenPreparada:
    simbolo: str
    side: str
    tipo: str
    cantidad: int
    time_in_force: str
    limit_price: float | None
    extended_hours: bool
    client_order_id: str


@dataclass(frozen=True)
class ResultadoOrden:
    enviada: bool
    client_order_id: str
    order_id: str | None
    status: str
    filled_qty: float
    filled_avg_price: float | None
    mensaje: str


def preparar_buy(
    simbolo: str,
    *,
    ask: float | None,
    bid: float | None,
    cantidad: int,
    sesion,
    mercado: str = "regular",
    client_order_id: str = "",
) -> OrdenPreparada:
    if not simbolo:
        raise ValueError("simbolo requerido")
    if cantidad < 1:
        raise ValueError("cantidad debe ser >= 1")

    mercado = mercado.lower()
    sesion.validar(mercado)

    if sesion.modo == "market":
        return OrdenPreparada(
            simbolo=simbolo.upper(),
            side="buy",
            tipo="market",
            cantidad=cantidad,
            time_in_force="day",
            limit_price=None,
            extended_hours=False,
            client_order_id=client_order_id,
        )

    limit_price = precio_limit(
        ask=ask,
        bid=bid,
        referencia=sesion.referencia_limit,
        offset_dolares=sesion.offset_dolares,
    )
    if limit_price is None or limit_price <= 0:
        raise ValueError("quote sin ask/bid valido para orden limit")

    return OrdenPreparada(
        simbolo=simbolo.upper(),
        side="buy",
        tipo="limit",
        cantidad=cantidad,
        time_in_force="day",
        limit_price=limit_price,
        extended_hours=mercado in {"premarket", "postmarket"},
        client_order_id=client_order_id,
    )


def _attr(obj: Any, name: str, default=None):
    value = getattr(obj, name, default)
    return default if value is None else value


class AlpacaExecutor:
    """Ejecutor idempotente; no envia nada mientras config.enabled sea False."""

    def __init__(self, config, trading_client=None):
        self.config = config
        self.config.validar()
        self._client = trading_client
        self._lock = Lock()
        self._results: dict[str, ResultadoOrden] = {}

    def _get_client(self):
        if self._client is not None:
            return self._client
        if not self.config.enabled:
            return None
        # No cargamos credenciales desde el scanner ni desde logs.
        # Para activacion real/paper se inyectara el TradingClient desde la
        # capa de conexion autenticada.
        return None

    def preparar(self, simbolo, *, ask, bid, cantidad, mercado="regular", client_order_id="", sesion=None):
        if sesion is None:
            sesion = getattr(self.config, mercado)
        return preparar_buy(
            simbolo,
            ask=ask,
            bid=bid,
            cantidad=cantidad,
            sesion=sesion,
            mercado=mercado,
            client_order_id=client_order_id,
        )

    def enviar_buy(self, orden: OrdenPreparada) -> ResultadoOrden:
        if orden.side != "buy":
            return ResultadoOrden(False, orden.client_order_id, None, "rejected", 0.0, None, "only_buy_supported")
        if not orden.client_order_id:
            return ResultadoOrden(False, "", None, "rejected", 0.0, None, "client_order_id_required")

        with self._lock:
            previo = self._results.get(orden.client_order_id)
            if previo is not None:
                return previo

            client = self._get_client()
            if client is None:
                result = ResultadoOrden(
                    False, orden.client_order_id, None, "disabled", 0.0, None,
                    "execution_disabled_or_client_missing",
                )
                self._results[orden.client_order_id] = result
                return result

            try:
                order_data = self._build_request(orden)
                order = client.submit_order(order_data=order_data)
                result = self._resultado(order, "order_submitted")
            except Exception as exc:
                result = ResultadoOrden(
                    False, orden.client_order_id, None, "rejected", 0.0, None,
                    f"submit_error:{type(exc).__name__}:{exc}",
                )
            self._results[orden.client_order_id] = result
            return result

    def _build_request(self, orden: OrdenPreparada):
        from alpaca.trading.enums import OrderSide, TimeInForce
        if orden.tipo == "limit":
            from alpaca.trading.requests import LimitOrderRequest
            return LimitOrderRequest(
                symbol=orden.simbolo,
                qty=orden.cantidad,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                limit_price=orden.limit_price,
                extended_hours=orden.extended_hours,
                client_order_id=orden.client_order_id,
            )
        from alpaca.trading.requests import MarketOrderRequest
        return MarketOrderRequest(
            symbol=orden.simbolo,
            qty=orden.cantidad,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            client_order_id=orden.client_order_id,
        )

    def consultar(self, client_order_id: str) -> ResultadoOrden | None:
        with self._lock:
            cached = self._results.get(client_order_id)
            client = self._get_client()
        if client is None:
            return cached
        try:
            order = client.get_order_by_client_id(client_order_id)
        except Exception:
            return cached
        result = self._resultado(order, "order_queried")
        with self._lock:
            self._results[client_order_id] = result
        return result

    def cancelar(self, client_order_id: str) -> ResultadoOrden | None:
        with self._lock:
            current = self._results.get(client_order_id)
            client = self._get_client()
        if client is None:
            return current
        if current and current.status in ESTADOS_TERMINALES:
            return current
        try:
            order = client.get_order_by_client_id(client_order_id)
            order_id = _attr(order, "id")
            if order_id is None:
                return current
            client.cancel_order_by_id(order_id)
            refreshed = self.consultar(client_order_id)
            return refreshed or current
        except Exception:
            return current

    def registrar_trade_update(self, order_payload) -> ResultadoOrden:
        result = self._resultado(order_payload, "trade_update")
        with self._lock:
            self._results[result.client_order_id] = result
        return result

    def resultado(self, client_order_id: str) -> ResultadoOrden | None:
        with self._lock:
            return self._results.get(client_order_id)

    @staticmethod
    def _resultado(order, mensaje: str) -> ResultadoOrden:
        client_id = str(_attr(order, "client_order_id", ""))
        order_id = _attr(order, "id")
        status = str(_attr(order, "status", "unknown")).lower()
        filled_qty_raw = _attr(order, "filled_qty", 0)
        avg_raw = _attr(order, "filled_avg_price")
        try:
            filled_qty = float(filled_qty_raw or 0)
        except (TypeError, ValueError):
            filled_qty = 0.0
        try:
            avg_price = float(avg_raw) if avg_raw is not None else None
        except (TypeError, ValueError):
            avg_price = None
        return ResultadoOrden(
            enviada=True,
            client_order_id=client_id,
            order_id=str(order_id) if order_id is not None else None,
            status=status,
            filled_qty=filled_qty,
            filled_avg_price=avg_price,
            mensaje=mensaje,
        )
