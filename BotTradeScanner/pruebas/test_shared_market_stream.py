from datetime import datetime, timezone
from types import SimpleNamespace

from BotTradeScanner.motor_velas.motor_velas import MotorVelas


class FakeMarketStream:
    def __init__(self):
        self.trade_callback = None
        self.quote_callback = None

    def add_consumer(self, trade_callback=None, quote_callback=None):
        self.trade_callback = trade_callback
        self.quote_callback = quote_callback

    def remove_consumer(self, trade_callback=None, quote_callback=None):
        if self.trade_callback == trade_callback:
            self.trade_callback = None
        if self.quote_callback == quote_callback:
            self.quote_callback = None

    def emit_trade(self, symbol, price, size, timestamp):
        self.trade_callback(
            SimpleNamespace(
                symbol=symbol,
                price=price,
                size=size,
                timestamp=timestamp,
            )
        )

    def emit_quote(self, symbol, bid, ask, timestamp):
        self.quote_callback(
            SimpleNamespace(
                symbol=symbol,
                bid_price=bid,
                ask_price=ask,
                timestamp=timestamp,
            )
        )


def test_motor_velas_usa_el_stream_compartido_sin_abrir_otro():
    motor = MotorVelas("key", "secret")
    stream = FakeMarketStream()

    motor.conectar_stream_compartido(stream)

    assert motor._stream_compartido is stream
    assert motor._stream is None
    assert motor._iniciado is True

    momento = datetime(2026, 10, 6, 14, 30, 10, tzinfo=timezone.utc)
    stream.emit_trade("TEST", 10.25, 100, momento)
    stream.emit_quote("TEST", 10.24, 10.26, momento)

    snap = motor.snapshot_simbolo("TEST")

    assert snap["vela_actual"]["apertura"] == 10.25
    assert snap["vela_actual"]["cierre"] == 10.25
    assert snap["bid"] == 10.24
    assert snap["ask"] == 10.26
    assert motor.total_trades == 1
    assert motor.ultimo_trade == momento

def test_motor_velas_iniciar_con_stream_compartido_no_crea_stock_data_stream():
    motor = MotorVelas("key", "secret")
    stream = FakeMarketStream()
    motor.conectar_stream_compartido(stream)

    llamadas = []

    def fake_precargar(simbolos, cantidad=300):
        llamadas.append((list(simbolos), cantidad))

    motor.precargar_historial = fake_precargar
    motor._iniciado = False

    motor.iniciar(["AAPL"])

    assert motor._stream is None
    assert motor._stream_compartido is stream
    assert llamadas == [(["AAPL"], 300)]
