from BotTradeScanner.ejecucion.configuracion import (
    ExecutionConfig, SesionEjecucion, cantidad_por_presupuesto, precio_limit,
)
from BotTradeScanner.ejecucion.alpaca import preparar_buy


def test_por_defecto_es_limit_al_ask():
    cfg = ExecutionConfig.por_defecto()
    assert cfg.premarket.modo == "limit"
    assert cfg.premarket.referencia_limit == "ask"
    assert precio_limit(ask=10.12, bid=10.10) == 10.12


def test_referencias_limit():
    assert precio_limit(ask=10.12, bid=10.10, referencia="ask_plus_0_01") == 10.13
    assert precio_limit(ask=10.12, bid=10.10, referencia="ask_plus_0_02") == 10.14
    assert precio_limit(ask=10.12, bid=10.10, referencia="bid") == 10.10
    assert precio_limit(ask=10.12, bid=10.10, referencia="bid_minus_0_01") == 10.09
    assert precio_limit(ask=10.12, bid=10.10, referencia="bid_minus_0_02") == 10.08


def test_extended_hours_rechaza_market():
    try:
        SesionEjecucion(modo="market").validar("premarket")
    except ValueError:
        pass
    else:
        raise AssertionError("market no debe validarse en extended hours")


def test_preparar_limit_al_ask():
    orden = preparar_buy(
        "AAPL", ask=10.12, bid=10.10, cantidad=10,
        sesion=SesionEjecucion(), mercado="premarket", client_order_id="x1",
    )
    assert orden.tipo == "limit"
    assert orden.limit_price == 10.12
    assert orden.extended_hours is True
    assert orden.time_in_force == "day"


def test_presupuesto():
    assert cantidad_por_presupuesto(12.4, max_dolares=120) == 9
    assert cantidad_por_presupuesto(12.4, max_dolares=120, max_acciones=5) == 5
