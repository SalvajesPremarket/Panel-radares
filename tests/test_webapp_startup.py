import sqlite3

import pytest

from webapp import storage
from webapp.server import startup


def test_startup_initializes_account_recommendations_schema(tmp_path, monkeypatch):
    database_path = tmp_path / "tradescanner.sqlite3"
    monkeypatch.setattr(storage, "SQLITE_PATH", database_path)
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)

    startup()

    with sqlite3.connect(database_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            )
        }

    assert {"users", "sessions", "recommendations"} <= tables
    assert "idx_recommendations_user" in indexes


def test_render_requires_persistent_database_url(monkeypatch):
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.setenv("RENDER", "true")

    with pytest.raises(RuntimeError, match="DATABASE_URL is required on Render"):
        storage.db()


from BotTradeScanner.riesgo.paper import PaperBot
from webapp.api import server


def test_tradebot_status_explicitly_disables_real_trading(monkeypatch):
    monkeypatch.setattr(server, "_paper_bots", {})
    status = server.tradebot_status(user={"account_status": "admin", "user_id": "paper-test-user"})
    assert status["mode"] == "paper"
    assert status["paper_simulator_connected"] is True
    assert status["real_trading_enabled"] is False
    assert status["broker_connected"] is False
    assert status["strategy_engine_connected"] is False


def test_manual_paper_buy_and_exit_never_enable_real_trading(monkeypatch):
    monkeypatch.setattr(server, "_paper_bots", {})
    user = {"account_status": "admin", "user_id": "paper-test-user"}
    buy = server.tradebot_evaluate(
        server.PaperTradeIn(symbol="aapl", action="BUY", price=10, stop_loss=9),
        user=user,
    )
    assert buy["mode"] == "paper"
    assert buy["real_trading_enabled"] is False
    assert buy["result"]["action"] == "buy"
    assert buy["result"]["simbolo"] == "AAPL"

    sell = server.tradebot_evaluate(
        server.PaperTradeIn(symbol="AAPL", action="EXIT", price=11),
        user=user,
    )
    assert sell["real_trading_enabled"] is False
    assert sell["result"]["action"] == "sell"
    assert sell["result"]["pnl_realizado"] > 0


def test_paper_buy_requires_stop_loss(monkeypatch):
    from fastapi import HTTPException
    monkeypatch.setattr(server, "_paper_bots", {})
    try:
        server.tradebot_evaluate(
            server.PaperTradeIn(symbol="AAPL", action="BUY", price=10),
            user={"account_status": "admin", "user_id": "paper-test-user"},
        )
    except HTTPException as exc:
        assert exc.status_code == 422
    else:
        raise AssertionError("BUY sin Stop Loss debe rechazarse")


from webapp.tradebot.runtime import TradeBotPaperRuntime


def test_automatic_runtime_stays_disabled_without_explicit_opt_in(monkeypatch):
    monkeypatch.delenv("TRADESCANNER_TRADEBOT_AUTO_PAPER", raising=False)
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    runtime = TradeBotPaperRuntime()

    runtime.start_if_configured()
    status = runtime.status()

    assert status["state"] == "disabled"
    assert status["real_trading_enabled"] is False
    assert status["broker_order_executor_created"] is False
    assert status["thread_alive"] is False


def test_automatic_runtime_requires_both_market_data_credentials(monkeypatch):
    monkeypatch.setenv("TRADESCANNER_TRADEBOT_AUTO_PAPER", "true")
    monkeypatch.setenv("ALPACA_API_KEY", "example-key")
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    runtime = TradeBotPaperRuntime()

    runtime.start_if_configured()
    status = runtime.status()

    assert status["state"] == "needs_credentials"
    assert status["credentials_configured"] is False
    assert status["real_trading_enabled"] is False
    assert status["thread_alive"] is False

def test_signals_survive_store_recreation(tmp_path, monkeypatch):
    from webapp.api.signal_service import Signal, SignalStore
    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "signals.sqlite3")
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)

    original = Signal(
        symbol="aapl",
        timeframe="1m",
        signal_type="SCANNER_FINAL",
        price=12.5,
        timestamp="2026-10-10T12:00:00+00:00",
        confidence=82,
        scanner_conditions={"gap_pct": 4.2},
    )
    SignalStore().publish(original)
    restored = SignalStore().list(symbol="AAPL", limit=10)

    assert len(restored) == 1
    assert restored[0]["signal_id"] == original.signal_id
    assert restored[0]["symbol"] == "AAPL"
    assert restored[0]["confidence"] == 82
    assert restored[0]["scanner_conditions"] == {"gap_pct": 4.2}


def test_manual_paper_positions_survive_bot_cache_reset(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "paper.sqlite3")
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.setattr(server, "_paper_bots", {})
    user = {"account_status": "admin", "user_id": "paper-persistence-test"}

    result = server.tradebot_evaluate(
        server.PaperTradeIn(symbol="AAPL", action="BUY", price=10, stop_loss=9),
        user=user,
    )
    assert result["result"]["action"] == "buy"

    # Simulate a web process restart by discarding the in-memory bot registry.
    monkeypatch.setattr(server, "_paper_bots", {})
    restored = server.paper_bot_for(user)

    assert len(restored.posiciones()) == 1
    assert restored.posiciones()[0]["simbolo"] == "AAPL"
    assert restored.decisions[-1]["action"] == "buy"
