"""Integration regressions for the current mainline shared stream -> MotorVelas bridge."""

import asyncio
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
        assert status["stream_compartido"] is True
        assert status["stream_trades"] == 1
        assert status["trade_consumer_errors"] == 0
        assert status["quote_consumer_errors"] == 0
    finally:
        motor.desconectar_stream_compartido()


def test_bridge_sync_results_uses_shared_stream_without_second_connection(monkeypatch):
    stream = AlpacaMarketStream("test-key", "test-secret", feed="iex")
    motor = MotorVelas("test-key", "test-secret")
    monkeypatch.setattr(motor, "precargar_historial", lambda symbols, cantidad=300: None)
    bridge = MotorVelasBridge("test-key", "test-secret", motor=motor, market_stream=stream)
    starts = []
    monkeypatch.setattr(stream, "start", lambda symbols: starts.append(list(symbols)))

    try:
        bridge.sync_results([{"ticker": "aapl"}, {"ticker": "MSFT"}, {"ticker": "AAPL"}])
        assert starts == [["AAPL", "MSFT"]]
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
