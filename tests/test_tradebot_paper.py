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
    buy = server.tradebot_evaluate(
        server.PaperTradeIn(symbol="aapl", action="BUY", price=10, stop_loss=9),
        user={"account_status": "admin", "user_id": "paper-test-user"},
    )
    assert buy["mode"] == "paper"
    assert buy["real_trading_enabled"] is False
    assert buy["result"]["action"] == "buy"
    assert buy["result"]["simbolo"] == "AAPL"

    sell = server.tradebot_evaluate(
        server.PaperTradeIn(symbol="AAPL", action="EXIT", price=11),
        user={"account_status": "admin", "user_id": "paper-test-user"},
    )
    assert sell["real_trading_enabled"] is False
    assert sell["result"]["action"] == "sell"
    assert sell["result"]["pnl_realizado"] > 0


def test_paper_buy_requires_stop_loss():
    from fastapi import HTTPException
    try:
        server.tradebot_evaluate(
            server.PaperTradeIn(symbol="AAPL", action="BUY", price=10),
            user={"account_status": "admin", "user_id": "paper-test-user"},
        )
    except HTTPException as exc:
        assert exc.status_code == 422
    else:
        raise AssertionError("BUY sin Stop Loss debe rechazarse")
