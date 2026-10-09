"""Regression tests for historical preload ordering in the shared live bridge."""

import time

from BotTradeScanner.integracion.live_motor_bridge import MotorVelasBridge
from BotTradeScanner.motor_velas.motor_velas import MotorVelas


def test_shared_stream_subscribes_new_symbol_only_after_history_preload(monkeypatch):
    motor = MotorVelas("test-key", "test-secret")
    starts = []

    class FakeSharedStream:
        def add_consumer(self, trade_callback=None, quote_callback=None):
            self.trade_callback = trade_callback
            self.quote_callback = quote_callback

        def remove_consumer(self, trade_callback=None, quote_callback=None):
            pass

        def start(self, symbols):
            symbols = list(symbols)
            if symbols:
                assert all(symbol in motor._historial_precargado for symbol in symbols)
            starts.append(symbols)

        def health_snapshot(self):
            return {"running": bool(starts and starts[-1]), "connected": False}

    stream = FakeSharedStream()

    def preload(symbols, cantidad=300):
        for symbol in symbols:
            motor._historial_precargado.add(symbol)
            motor._obtener_motor(symbol)

    monkeypatch.setattr(motor, "precargar_historial", preload)
    bridge = MotorVelasBridge(
        "test-key", "test-secret", motor=motor, market_stream=stream
    )

    try:
        bridge.sync_results([{"ticker": "AAPL"}])

        deadline = time.time() + 3
        while not any(call == ["AAPL"] for call in starts) and time.time() < deadline:
            time.sleep(0.01)

        assert starts[0] == []
        assert ["AAPL"] in starts
        assert "AAPL" in motor._historial_precargado
        assert "AAPL" in motor.motores
    finally:
        motor.desconectar_stream_compartido()
