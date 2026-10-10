import pytest

from webapp.tradebot.runtime import TradeBotPaperRuntime


def test_runtime_config_rejects_unsafe_risk_values():
    runtime = TradeBotPaperRuntime()
    with pytest.raises(ValueError, match="Stop Loss"):
        runtime.configure({
            "capital_asignado": 600,
            "porcentaje_operacion": 20,
            "stop_loss_pct": 0,
            "take_profit_pct": 4,
        })
    with pytest.raises(ValueError, match="Take Profit"):
        runtime.configure({
            "capital_asignado": 600,
            "porcentaje_operacion": 20,
            "stop_loss_pct": 2,
            "take_profit_pct": 0,
        })


def test_manual_start_requires_market_data_credentials(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    monkeypatch.delenv("TRADESCANNER_TRADEBOT_AUTO_PAPER", raising=False)
    runtime = TradeBotPaperRuntime()
    status = runtime.start_manual({
        "capital_asignado": 800,
        "porcentaje_operacion": 15,
        "stop_loss_pct": 1.5,
        "take_profit_pct": 3.0,
        "estrategia": "LongSalvajesPreMarket",
    })
    assert status["state"] == "needs_credentials"
    assert status["real_trading_enabled"] is False
    assert status["broker_order_executor_created"] is False
    assert status["config_operativa"]["capital_asignado"] == 800
    assert status["thread_alive"] is False
    runtime.stop_manual()



def test_runtime_accepts_pullback_short_strategy_in_paper(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    monkeypatch.delenv("TRADESCANNER_TRADEBOT_AUTO_PAPER", raising=False)
    runtime = TradeBotPaperRuntime()
    config = runtime.configure({
        "capital_asignado": 600,
        "porcentaje_operacion": 20,
        "stop_loss_pct": 2,
        "take_profit_pct": 4,
        "estrategia": "Pullback corto ema50 ó 200 día ó semana",
    })
    assert config["estrategia"] == "Pullback corto ema50 ó 200 día ó semana"
    assert runtime.status()["real_trading_enabled"] is False

def test_pullback_paper_runtime_state_survives_restart(tmp_path, monkeypatch):
    from webapp import storage
    from webapp.tradebot.pullback_corto import PullbackCortoEMA

    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "runtime.sqlite3")
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)
    config = {
        "capital_asignado": 600,
        "porcentaje_operacion": 20,
        "stop_loss_pct": 2,
        "take_profit_pct": 4,
        "estrategia": "Pullback corto ema50 ó 200 día ó semana",
    }

    first = TradeBotPaperRuntime()
    first.configure(config)
    strategy = PullbackCortoEMA()
    strategy.position_open = True
    strategy.entry_price = 12.5
    strategy.stop_loss = 12.8
    strategy.last_previous_candle = (12.4, 13.0, 12.0, 12.6)
    strategy.last_symbol = "AAPL"
    strategy.last_price = 12.55
    first._short_position = {
        "symbol": "AAPL",
        "entry_price": 12.5,
        "stop_loss": 12.8,
        "quantity": 9,
        "strategy": config["estrategia"],
    }
    first._strategy_decisions = [{"symbol": "AAPL", "action": "SHORT", "mode": "paper"}]
    first._persist_short_runtime_state(strategy, force=True)

    restarted = TradeBotPaperRuntime()
    restarted.configure(config)
    restored_strategy = PullbackCortoEMA()
    restored = restarted._restore_short_runtime_state(restored_strategy)

    assert restored["strategy"] == config["estrategia"]
    assert restarted._short_position["symbol"] == "AAPL"
    assert restarted._short_position["quantity"] == 9
    assert restored_strategy.position_open is True
    assert restored_strategy.entry_price == 12.5
    assert restored_strategy.stop_loss == 12.8
    assert restored_strategy.last_previous_candle == (12.4, 13.0, 12.0, 12.6)
    assert restarted._strategy_decisions[-1]["action"] == "SHORT"

def test_runtime_ignores_stale_scanner_candidates():
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    fresh = (now - timedelta(seconds=20)).isoformat()
    stale = (now - timedelta(minutes=10)).isoformat()
    runtime = TradeBotPaperRuntime()

    result = runtime._fresh_signals([
        {"symbol": "AAPL", "timestamp": fresh},
        {"symbol": "MSFT", "timestamp": stale},
        {"symbol": "NVDA"},
        {"symbol": "TSLA", "timestamp": "not-a-timestamp"},
    ])

    assert [item["symbol"] for item in result] == ["AAPL"]

def test_long_paper_position_and_strategy_state_survive_runtime_restart(tmp_path, monkeypatch):
    from webapp import storage
    from BotTradeScanner.integracion.bot_long_realtime import BotLongRealtime
    from BotTradeScanner.estrategias.long.premarket_salvajes import EstadoLong

    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "runtime.sqlite3")
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)

    config = {
        "capital_asignado": 600,
        "porcentaje_operacion": 20,
        "stop_loss_pct": 2,
        "take_profit_pct": 4,
        "estrategia": "LongSalvajesPreMarket",
    }
    runtime = TradeBotPaperRuntime()
    runtime.configure(config)
    bot = BotLongRealtime(object())
    bot.configurar_riesgo(**config)

    strategy = bot.decisiones._estrategia("AAPL")
    strategy.estado = EstadoLong.LONG_PRIMERA_VELA
    strategy.simbolo = "AAPL"
    strategy.precio_entrada = 10.0
    strategy.stop_loss = 9.5
    bot.paper.evaluar({
        "signal_id": "paper-position-1",
        "simbolo": "AAPL",
        "accion": "BUY",
        "precio": 10.0,
        "stop_loss": 9.5,
        "estrategia": "PreMarketSalvajes LONG",
    })
    runtime._persist_long_runtime_state(bot, force=True)

    restarted_runtime = TradeBotPaperRuntime()
    restarted_bot = BotLongRealtime(object())
    assert restarted_runtime._restore_long_runtime_state(restarted_bot) is True

    assert restarted_bot.paper.posiciones()[0]["simbolo"] == "AAPL"
    assert restarted_bot.paper.posiciones()[0]["precio_entrada"] == 10.0
    assert restarted_bot.decisiones.estado("AAPL")["estado"] == "long_primera_vela"
    assert restarted_bot.decisiones.estado("AAPL")["stop_loss"] == 9.5
    assert restarted_bot.paper.status()["modo"] == "paper"
    assert restarted_runtime._operational_config["capital_asignado"] == 600

def test_corrupt_paper_runtime_state_fails_closed(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from webapp import storage

    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "corrupt-runtime.sqlite3")
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)
    runtime = TradeBotPaperRuntime()
    runtime._init_runtime_state_schema()
    with storage.db() as conn:
        conn.execute(
            "INSERT INTO tradebot_runtime_state(runtime_id,state_json,updated_at) VALUES(?,?,?)",
            ("singleton", "{not-json", datetime.now(timezone.utc).isoformat()),
        )

    with pytest.raises(RuntimeError, match="no se puede leer"):
        runtime._read_runtime_state()


def test_corrupt_short_position_snapshot_fails_closed(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from webapp import storage
    from webapp.tradebot.pullback_corto import PullbackCortoEMA

    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "corrupt-short-runtime.sqlite3")
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)

    runtime = TradeBotPaperRuntime()
    runtime.configure({
        "capital_asignado": 600,
        "porcentaje_operacion": 20,
        "stop_loss_pct": 2,
        "take_profit_pct": 4,
        "estrategia": "Pullback corto ema50 ó 200 día ó semana",
    })
    runtime._init_runtime_state_schema()
    corrupt = {
        "strategy": "Pullback corto ema50 ó 200 día ó semana",
        "short_position": {"symbol": "AAPL"},
        "strategy_state": {},
        "strategy_decisions": [],
    }
    import json
    with storage.db() as conn:
        conn.execute(
            "INSERT INTO tradebot_runtime_state(runtime_id,state_json,updated_at) VALUES(?,?,?)",
            ("singleton", json.dumps(corrupt), datetime.now(timezone.utc).isoformat()),
        )

    with pytest.raises(RuntimeError, match="posición corta Paper guardada está incompleta"):
        runtime._restore_short_runtime_state(PullbackCortoEMA())
