from BotTradeScanner.riesgo.paper import PaperBot, RiskConfig


def test_paper_abre_long_con_stop_de_estrategia():
    bot = PaperBot()
    d = bot.evaluar({
        "signal_id": "s1",
        "simbolo": "TEST",
        "accion": "BUY",
        "precio": 10.0,
        "stop_loss": 9.5,
        "estrategia": "PreMarketSalvajes LONG",
    })
    assert d["action"] == "buy"
    assert bot.posiciones()[0]["stop_loss"] == 9.5


def test_paper_no_usa_take_profit_porcentual_y_sube_stop():
    bot = PaperBot()
    bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 10.0, "stop_loss": 9.5,
    })
    d = bot.evaluar({
        "signal_id": "s2", "simbolo": "TEST", "accion": "HOLD",
        "precio": 11.0, "stop_loss": 10.2,
    })
    assert d["action"] == "hold"
    assert bot.posiciones()[0]["stop_loss"] == 10.2


def test_paper_no_baja_stop():
    bot = PaperBot()
    bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 10.0, "stop_loss": 9.5,
    })
    bot.evaluar({
        "signal_id": "s2", "simbolo": "TEST", "accion": "HOLD",
        "precio": 10.5, "stop_loss": 9.0,
    })
    assert bot.posiciones()[0]["stop_loss"] == 9.5


def test_paper_exit_cierra_posicion():
    bot = PaperBot(RiskConfig(max_simultaneous_positions=1))
    bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 10.0, "stop_loss": 9.5,
    })
    d = bot.evaluar({
        "signal_id": "s2", "simbolo": "TEST", "accion": "EXIT",
        "precio": 10.8, "motivo": "Bollinger superior + lapida",
    })
    assert d["action"] == "sell"
    assert bot.posiciones() == []


def test_paper_limita_posiciones():
    bot = PaperBot(RiskConfig(max_simultaneous_positions=1))
    bot.evaluar({
        "signal_id": "s1", "simbolo": "AAA", "accion": "BUY",
        "precio": 10.0, "stop_loss": 9.5,
    })
    d = bot.evaluar({
        "signal_id": "s2", "simbolo": "BBB", "accion": "BUY",
        "precio": 20.0, "stop_loss": 19.0,
    })
    assert d["action"] == "blocked"
    assert d["reason"] == "max_simultaneous_positions"
