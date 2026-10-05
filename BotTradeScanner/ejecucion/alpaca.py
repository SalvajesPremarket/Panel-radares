"""Constructor seguro de ordenes Alpaca.

No envia ordenes por defecto. Solo prepara la orden y valida las reglas de
regular/extended hours. La activacion de ordenes reales se hara despues de
auditar fills, cancelaciones, desconexion y duplicados.
"""
from __future__ import annotations

from dataclasses import dataclass


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

    from BotTradeScanner.ejecucion.configuracion import precio_limit

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
