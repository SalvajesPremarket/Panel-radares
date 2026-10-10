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
