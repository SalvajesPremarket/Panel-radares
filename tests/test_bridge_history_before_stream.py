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


def test_candidate_removed_during_history_preload_does_not_leave_stale_motor(monkeypatch):
    import threading

    motor = MotorVelas("test-key", "test-secret")
    preload_started = threading.Event()
    allow_preload_to_finish = threading.Event()

    class FakeSharedStream:
        def add_consumer(self, trade_callback=None, quote_callback=None):
            pass

        def remove_consumer(self, trade_callback=None, quote_callback=None):
            pass

        def start(self, symbols):
            pass

        def health_snapshot(self):
            return {"running": False, "connected": False}

    def preload(symbols, cantidad=300):
        for symbol in symbols:
            motor._historial_precargado.add(symbol)
            motor._obtener_motor(symbol)
        preload_started.set()
        assert allow_preload_to_finish.wait(timeout=3)

    monkeypatch.setattr(motor, "precargar_historial", preload)
    bridge = MotorVelasBridge(
        "test-key", "test-secret", motor=motor, market_stream=FakeSharedStream()
    )

    try:
        bridge.sync_results([{"ticker": "AAPL"}])
        assert preload_started.wait(timeout=2)
        bridge.sync_results([])
        allow_preload_to_finish.set()

        deadline = time.time() + 3
        while ("AAPL" in motor.motores or "AAPL" in motor._historial_precargado) and time.time() < deadline:
            time.sleep(0.01)

        assert "AAPL" not in motor.motores
        assert "AAPL" not in motor._historial_precargado
    finally:
        allow_preload_to_finish.set()
        motor.desconectar_stream_compartido()
