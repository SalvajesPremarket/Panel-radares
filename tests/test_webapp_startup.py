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

def test_only_admin_can_control_global_paper_runtime():
    from fastapi import HTTPException

    assert server.runtime_operator(user={"role": "admin", "account_status": "admin"})["role"] == "admin"
    for role in ("user", "support", ""):
        try:
            server.runtime_operator(user={"role": role, "account_status": "active_monthly"})
        except HTTPException as exc:
            assert exc.status_code == 403
        else:
            raise AssertionError(f"El rol {role!r} no debe controlar el runtime global")


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


def test_corrupt_manual_paper_state_fails_closed(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from fastapi import HTTPException

    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "corrupt-paper.sqlite3")
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.setattr(server, "_paper_bots", {})
    user = {"account_status": "admin", "user_id": "corrupt-paper-user"}

    server.init_paper_schema()
    with storage.db() as conn:
        conn.execute(
            "INSERT INTO paper_bot_states(user_id, state_json, updated_at) VALUES(?,?,?)",
            ("corrupt-paper-user", "{not-json", datetime.now(timezone.utc).isoformat()),
        )

    with pytest.raises(RuntimeError, match="se bloqueó la recuperación"):
        server.paper_bot_for(user)



def test_signal_ingest_rejects_missing_or_invalid_shared_secret(monkeypatch):
    from fastapi import HTTPException

    monkeypatch.setenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "expected-secret")
    for supplied in (None, "wrong-secret"):
        try:
            server.require_ingest_key(x_tradescanner_signal_key=supplied)
        except HTTPException as exc:
            assert exc.status_code == 401
        else:
            raise AssertionError("La API debe rechazar una clave de ingestión incorrecta")


def test_authenticated_signal_ingest_persists_signal_for_tradebot(tmp_path, monkeypatch):
    from webapp.api.signal_service import store

    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "ingest.sqlite3")
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.setenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "expected-secret")

    server.require_ingest_key(x_tradescanner_signal_key="expected-secret")
    payload = server.SignalBatchIn(items=[
        server.SignalIn(
            symbol="aapl",
            timeframe="1m",
            signal_type="SCANNER_FINAL",
            price=12.34,
            confidence=87,
            scanner_conditions={"gap_pct": 4.5, "ema20": 9.7, "ema50_dia": 10.5},
        )
    ])
    result = server.ingest_signals(payload, None)
    stored = store.list(symbol="AAPL", limit=10)

    assert result["accepted"] == 1
    assert len(result["signal_ids"]) == 1
    assert len(stored) == 1
    assert stored[0]["signal_id"] == result["signal_ids"][0]
    assert stored[0]["price"] == 12.34
    assert stored[0]["confidence"] == 87
    assert stored[0]["scanner_conditions"]["ema50_dia"] == 10.5

    from webapp.tradebot.pullback_corto import PullbackCortoEMA

    snapshot = {
        "simbolo": "AAPL",
        "tramo_actual": 1,
        "market_data_trade_age_sec": 0.1,
        "vela_actual": {"apertura": 9.8, "maximo": 11.0, "minimo": 9.75, "cierre": 9.79},
        "vela_anterior": {"apertura": 10.0, "maximo": 12.0, "minimo": 9.9, "cierre": 9.95},
    }
    decision = PullbackCortoEMA().evaluate(snapshot, stored[0]["scanner_conditions"])
    assert decision["accion"] == "SHORT"
    assert decision["position_open"] is True
    assert decision["stop_loss"] == 9.95
