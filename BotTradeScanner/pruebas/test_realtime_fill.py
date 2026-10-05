from dataclasses import dataclass

from BotTradeScanner.integracion.bot_long_realtime import BotLongRealtime
from BotTradeScanner.ejecucion.configuracion import ExecutionConfig


@dataclass
class FakeResult:
    enviada: bool
    client_order_id: str
    order_id: str
    status: str
    filled_qty: float
    filled_avg_price: float | None
    mensaje: str


class FakeExecutor:
    def __init__(self):
        self.status = "new"
        self.client_order_id = None

    def preparar(self, simbolo, *, ask, bid, cantidad, mercado, client_order_id, sesion):
        self.client_order_id = client_order_id
        return type("Order", (), {
            "tipo": "limit",
            "limit_price": float(ask),
            "extended_hours": False,
            "time_in_force": "day",
            "client_order_id": client_order_id,
        })()

    def enviar_buy(self, orden):
        return FakeResult(
            True, orden.client_order_id, "order-1", self.status,
            0.0, None, "submitted",
        )

    def consultar(self, client_order_id):
        if self.status == "filled":
            return FakeResult(
                True, client_order_id, "order-1", "filled",
                1.0, 10.01, "order_queried",
            )
        return FakeResult(
            True, client_order_id, "order-1", self.status,
            0.0, None, "order_queried",
        )


def _snap():
    return {
        "simbolo": "TEST",
        "sin_datos": False,
        "tramo_actual": 1,
        "bid": 10.00,
        "ask": 10.01,
        "vela_actual": {
            "apertura": 10.0,
            "maximo": 10.1,
            "minimo": 9.7,
            "cierre": 10.0,
            "es_positiva": False,
            "es_libelula_en_curso": True,
            "es_lapida_en_curso": False,
            "regreso_a_apertura": True,
        },
        "vela_anterior": {"minimo": 9.0, "maximo": 10.5, "cierre": 10.2},
        "minimo_supera_anterior": True,
        "maximo_supera_anterior": False,
        "ema9": 10.1,
        "ema20": 9.8,
        "ema50": 7.5,
        "ema200": 6.5,
        "ema9_anterior": 10.1,
        "ema20_anterior": 9.8,
        "ema50_anterior": 7.5,
        "ema200_anterior": 6.5,
        "macd": 0.2,
        "macd_anterior": 0.2,
        "banda_bollinger_superior": 12.0,
        "banda_bollinger_superior_anterior": 12.0,
        "banda_bollinger_inferior": 8.0,
        "banda_bollinger_inferior_anterior": 8.0,
    }


def test_buy_queda_pendiente_hasta_filled():
    class Bridge:
        def sync_results(self, rows):
            pass

        def snapshot(self, simbolo):
            return _snap()

    bridge = Bridge()
    executor = FakeExecutor()
    cfg = ExecutionConfig.por_defecto()
    cfg.enabled = True
    bot = BotLongRealtime(bridge, executor=executor, execution_config=cfg)

    bot.sync_candidates([{"ticker": "TEST"}])

    # Primer contacto: la maquina registra el candidato y devuelve WATCH.
    first = bot.evaluar_ahora()
    assert first
    assert first[-1]["accion"] == "WATCH"
    assert bot.paper.posiciones() == []

    # Segundo contacto: la libelula ya estaba formada y ahora se envia BUY.
    second = bot.evaluar_ahora()
    assert second
    assert second[-1]["accion"] == "WAIT"
    assert second[-1]["motivo"] == "buy_order_submitted_waiting_fill"
    assert bot.paper.posiciones() == []

    executor.status = "filled"
    second = bot.evaluar_ahora()

    assert any(x.get("motivo") == "BUY confirmado por FILLED Alpaca Paper" for x in second)
    posiciones = bot.paper.posiciones()
    assert len(posiciones) == 1
    assert posiciones[0]["simbolo"] == "TEST"
    assert posiciones[0]["precio_entrada"] == 10.01


def test_realtime_usa_tamano_de_riesgo_para_la_orden():
    class Bridge:
        def sync_results(self, rows):
            pass

        def snapshot(self, simbolo):
            return _snap()

    class CapturingExecutor(FakeExecutor):
        def __init__(self):
            super().__init__()
            self.cantidad_enviada = None

        def preparar(self, simbolo, *, ask, bid, cantidad, mercado, client_order_id, sesion):
            self.cantidad_enviada = cantidad
            return super().preparar(
                simbolo, ask=ask, bid=bid, cantidad=cantidad,
                mercado=mercado, client_order_id=client_order_id, sesion=sesion,
            )

    bridge = Bridge()
    executor = CapturingExecutor()
    cfg = ExecutionConfig.por_defecto()
    cfg.enabled = True
    bot = BotLongRealtime(bridge, executor=executor, execution_config=cfg)
    bot.sync_candidates([{"ticker": "TEST"}])

    bot.evaluar_ahora()
    bot.evaluar_ahora()

    # Ask=10.01 y stop=10.00: la exposicion maxima de $120 limita la orden a 11 acciones.
    assert executor.cantidad_enviada == 11
