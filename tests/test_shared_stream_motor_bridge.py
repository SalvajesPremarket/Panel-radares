"""Integration regressions for the current mainline shared stream -> MotorVelas bridge."""

import asyncio
import time
from datetime import datetime, timezone
from types import SimpleNamespace

from BotTradeScanner.integracion.live_motor_bridge import MotorVelasBridge
from BotTradeScanner.motor_velas.motor_velas import MotorVelas
from TradeScanner.data_engine import AlpacaMarketStream


def test_bridge_delivers_trade_and_quote_to_motor_velas():
    stream = AlpacaMarketStream("test-key", "test-secret", feed="iex")
    motor = MotorVelas("test-key", "test-secret")
    bridge = MotorVelasBridge("test-key", "test-secret", motor=motor, market_stream=stream)
    timestamp = datetime(2026, 10, 9, 16, 0, 15, tzinfo=timezone.utc)
    trade = SimpleNamespace(symbol="AAPL", price=10.25, size=50, timestamp=timestamp)
    quote = SimpleNamespace(
        symbol="AAPL", bid_price=10.20, ask_price=10.30, timestamp=timestamp
    )

    try:
        asyncio.run(stream._trade(trade))
        asyncio.run(stream._quote(quote))

        snapshot = bridge.snapshot("AAPL")
        status = bridge.status()
        assert motor.total_trades == 1
        assert motor.ultimo_trade == timestamp
        assert snapshot["vela_actual"]["cierre"] == 10.25
        assert motor.motores["AAPL"].vela_actual.volumen == 50
        assert snapshot["bid"] == 10.20
        assert snapshot["ask"] == 10.30
        assert snapshot["market_data_trade_age_sec"] is not None
        assert 0 <= snapshot["market_data_trade_age_sec"] < 2
        assert snapshot["market_data_quote_age_sec"] is not None
        assert 0 <= snapshot["market_data_quote_age_sec"] < 2
        assert status["stream_compartido"] is True
        assert status["stream_trades"] == 1
        assert status["trade_consumer_errors"] == 0
        assert status["quote_consumer_errors"] == 0
    finally:
        motor.desconectar_stream_compartido()


def test_bridge_sync_results_uses_shared_stream_without_second_connection(monkeypatch):
    stream = AlpacaMarketStream("test-key", "test-secret", feed="iex")
    motor = MotorVelas("test-key", "test-secret")
    def preload(symbols, cantidad=300):
        for symbol in symbols:
            motor._historial_precargado.add(symbol)
            motor._obtener_motor(symbol)

    monkeypatch.setattr(motor, "precargar_historial", preload)
    bridge = MotorVelasBridge("test-key", "test-secret", motor=motor, market_stream=stream)
    starts = []

    def record_start(symbols):
        symbols = list(symbols)
        if symbols:
            assert all(symbol in motor._historial_precargado for symbol in symbols)
        starts.append(symbols)

    monkeypatch.setattr(stream, "start", record_start)

    try:
        bridge.sync_results([{"ticker": "aapl"}, {"ticker": "MSFT"}, {"ticker": "AAPL"}])
        deadline = time.time() + 3
        while ["AAPL", "MSFT"] not in starts and time.time() < deadline:
            time.sleep(0.01)
        assert starts[0] == []
        assert ["AAPL", "MSFT"] in starts
        assert bridge.status()["stream_compartido"] is True
        assert motor._stream is None
        assert motor._stream_compartido is stream
    finally:
        motor.desconectar_stream_compartido()


def test_bridge_empty_results_clears_shared_stream_symbols():
    stream = AlpacaMarketStream("test-key", "test-secret", feed="iex")
    motor = MotorVelas("test-key", "test-secret")
    bridge = MotorVelasBridge("test-key", "test-secret", motor=motor, market_stream=stream)

    try:
        bridge.sync_results([])
        snapshot = stream.health_snapshot()
        assert snapshot["subscribed_symbols"] == []
        assert snapshot["running"] is False
        assert motor._stream is None
    finally:
        motor.desconectar_stream_compartido()



def test_market_stream_preserves_scanner_priority_within_symbol_cap():
    stream = AlpacaMarketStream("test-key", "test-secret", feed="iex", max_symbols=3)

    selected = stream._normalizar_simbolos(
        ["ZETA", "aapl", "ZETA", "MSFT", "AMD", "NVDA"]
    )

    assert selected == ["ZETA", "AAPL", "MSFT"]
