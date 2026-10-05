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


class FakeOrder:
    def __init__(self, client_order_id, status="new", filled_qty="0", filled_avg_price=None):
        self.id = "order-123"
        self.client_order_id = client_order_id
        self.status = status
        self.filled_qty = filled_qty
        self.filled_avg_price = filled_avg_price


class FakeClient:
    def __init__(self):
        self.submits = 0
        self.cancels = 0
        self.order = None

    def submit_order(self, order_data):
        self.submits += 1
        cid = order_data.client_order_id
        self.order = FakeOrder(cid, "new")
        return self.order

    def get_order_by_client_id(self, client_order_id):
        return self.order

    def cancel_order_by_id(self, order_id):
        self.cancels += 1
        self.order.status = "canceled"


def test_executor_disabled_no_envia():
    from BotTradeScanner.ejecucion.alpaca import AlpacaExecutor
    cfg = ExecutionConfig.por_defecto()
    ex = AlpacaExecutor(cfg, FakeClient())
    orden = ex.preparar(
        "AAPL", ask=10.12, bid=10.10, cantidad=2,
        mercado="regular", client_order_id="paper-1",
    )
    result = ex.enviar_buy(orden)
    assert result.status == "disabled"
    assert result.enviada is False


def test_executor_envia_una_sola_vez_y_registra_fill():
    from BotTradeScanner.ejecucion.alpaca import AlpacaExecutor
    cfg = ExecutionConfig.por_defecto()
    cfg.enabled = True
    client = FakeClient()
    ex = AlpacaExecutor(cfg, client)
    # El objetivo de esta prueba es idempotencia/seguimiento; desacoplamos
    # el test del modelo Pydantic concreto de alpaca-py.
    ex._build_request = lambda orden: orden
    orden = ex.preparar(
        "AAPL", ask=10.12, bid=10.10, cantidad=2,
        mercado="regular", client_order_id="paper-2",
    )
    first = ex.enviar_buy(orden)
    second = ex.enviar_buy(orden)
    assert first.status == "new"
    assert second == first
    assert client.submits == 1

    filled = FakeOrder("paper-2", "filled", "2", "10.13")
    tracked = ex.registrar_trade_update(filled)
    assert tracked.status == "filled"
    assert tracked.filled_qty == 2.0
    assert tracked.filled_avg_price == 10.13


def test_executor_cancelacion():
    from BotTradeScanner.ejecucion.alpaca import AlpacaExecutor
    cfg = ExecutionConfig.por_defecto()
    cfg.enabled = True
    client = FakeClient()
    ex = AlpacaExecutor(cfg, client)
    ex._build_request = lambda orden: orden
    orden = ex.preparar(
        "AAPL", ask=10.12, bid=10.10, cantidad=1,
        mercado="regular", client_order_id="paper-3",
    )
    ex.enviar_buy(orden)
    result = ex.cancelar("paper-3")
    assert client.cancels == 1
    assert result is not None
    assert result.status == "canceled"
