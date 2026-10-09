"""Integration regressions for the shared market stream -> MotorVelas bridge."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from BotTradeScanner.integracion.live_motor_bridge import MotorVelasBridge
from BotTradeScanner.motor_velas.motor_velas import MotorVelas
from TradeScanner.data_engine import AlpacaMarketStream


def test_bridge_keeps_motor_consumer_registered_and_receives_trade_and_quote():
    stream = AlpacaMarketStream("test-key", "test-secret", feed="iex")
    motor = MotorVelas("test-key", "test-secret")
    bridge = MotorVelasBridge("test-key", "test-secret", motor=motor, market_stream=stream)

    timestamp = datetime(2026, 10, 9, 16, 0, 15, tzinfo=timezone.utc)
    trade = SimpleNamespace(symbol="AAPL", price=10.25, size=50, timestamp=timestamp)
    quote = SimpleNamespace(
        symbol="AAPL", bid_price=10.20, ask_price=10.30, timestamp=timestamp
    )

    asyncio.run(stream._trade(trade))
    asyncio.run(stream._quote(quote))

    snapshot = bridge.snapshot("AAPL")
    status = bridge.status()
    assert motor.total_trades == 1
    assert motor.ultimo_trade == timestamp
    assert snapshot["vela_actual"]["cierre"] == 10.25
    assert snapshot["vela_actual"]["volumen"] == 50
    assert snapshot["bid"] == 10.20
    assert snapshot["ask"] == 10.30
    assert status["stream_compartido"] is True
    assert status["stream_trades"] == 1
    assert status["trade_consumer_errors"] == 0
    assert status["quote_consumer_errors"] == 0

    motor.desconectar_stream_compartido()


def test_bridge_sync_results_updates_shared_stream_symbols_without_second_stream(monkeypatch):
    stream = AlpacaMarketStream("test-key", "test-secret", feed="iex")
    motor = MotorVelas("test-key", "test-secret")
    # Historical preload is an external Alpaca API call; isolate this test to
    # the subscription/bridge contract and avoid network access.
    monkeypatch.setattr(motor, "precargar_historial", lambda symbols, cantidad=300: None)
    bridge = MotorVelasBridge("test-key", "test-secret", motor=motor, market_stream=stream)

    starts = []
    monkeypatch.setattr(stream, "start", lambda symbols: starts.append(list(symbols)))

    bridge.sync_results([{"ticker": "aapl"}, {"ticker": "MSFT"}, {"ticker": "AAPL"}])

    assert starts == [["AAPL", "MSFT"]]
    assert bridge.status()["stream_compartido"] is True
    # Shared mode must not create MotorVelas' own StockDataStream connection.
    assert motor._stream is None
    assert motor._stream_compartido is stream

    motor.desconectar_stream_compartido()


def test_bridge_empty_results_clears_shared_stream_subscriptions(monkeypatch):
    stream = AlpacaMarketStream("test-key", "test-secret", feed="iex")
    motor = MotorVelas("test-key", "test-secret")
    bridge = MotorVelasBridge("test-key", "test-secret", motor=motor, market_stream=stream)

    starts = []
    monkeypatch.setattr(stream, "start", lambda symbols: starts.append(list(symbols)))
    bridge.sync_results([])

    assert starts == [[]]
    assert stream.health_snapshot()["subscribed_symbols"] == []
    assert motor._stream is None
    motor.desconectar_stream_compartido()
