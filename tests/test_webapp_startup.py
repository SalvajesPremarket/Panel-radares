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
