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


def test_paper_calcula_tamano_con_600_dolares_y_riesgo_1_por_ciento():
    bot = PaperBot()
    d = bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 10.0, "stop_loss": 9.5,
    })
    # $6 de riesgo / $0.50 por accion = 12 acciones.
    # La exposicion es $120, exactamente el limite del 20% de $600.
    assert d["action"] == "buy"
    assert d["cantidad"] == 12.0
    assert d["capital_expuesto"] == 120.0
    assert d["riesgo_dolares"] == 6.0


def test_paper_limita_tamano_por_exposicion():
    bot = PaperBot()
    d = bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 5.0, "stop_loss": 4.0,
    })
    # Por riesgo serían 6 acciones; por exposicion tambien 24, asi que manda riesgo.
    assert d["cantidad"] == 6.0
    assert d["capital_expuesto"] == 30.0
    assert d["riesgo_dolares"] == 6.0


def test_paper_bloquea_si_el_stop_no_permite_una_accion():
    bot = PaperBot()
    d = bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 100.0, "stop_loss": 99.0,
    })
    # El riesgo permitiria 6 acciones, pero la exposicion maxima de $120 solo permite 1.
    assert d["cantidad"] == 1.0
    assert d["capital_expuesto"] == 100.0


def test_paper_bloquea_si_el_riesgo_por_una_accion_excede_el_maximo():
    bot = PaperBot()
    d = bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 10.0, "stop_loss": 3.0,
    })
    # Una accion arriesgaria $7, superior a los $6 permitidos.
    assert d["action"] == "blocked"
    assert d["reason"] == "position_size_below_one_share"


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
