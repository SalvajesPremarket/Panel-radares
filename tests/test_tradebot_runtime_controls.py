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
