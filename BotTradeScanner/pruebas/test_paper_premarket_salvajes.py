from BotTradeScanner.riesgo.paper import PaperBot, RiskConfig


def test_paper_abre_long_con_stop_de_estrategia():
    bot = PaperBot()
    d = bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 10.0, "stop_loss": 9.5,
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
    assert d["cantidad"] == 6.0
    assert d["capital_expuesto"] == 30.0
    assert d["riesgo_dolares"] == 6.0


def test_paper_bloquea_si_el_stop_no_permite_una_accion():
    bot = PaperBot()
    d = bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 100.0, "stop_loss": 99.0,
    })
    assert d["cantidad"] == 1.0
    assert d["capital_expuesto"] == 100.0


def test_paper_bloquea_si_el_riesgo_por_una_accion_excede_el_maximo():
    bot = PaperBot()
    d = bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 10.0, "stop_loss": 3.0,
    })
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


def test_paper_exit_cierra_posicion_y_calcula_pnl():
    bot = PaperBot()
    bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 10.0, "stop_loss": 9.5,
    })
    d = bot.evaluar({
        "signal_id": "s2", "simbolo": "TEST", "accion": "EXIT",
        "precio": 10.8, "motivo": "Bollinger superior + lapida",
    })
    assert d["action"] == "sell"
    assert d["pnl_realizado"] == 9.6
    assert bot.posiciones() == []
    assert bot.operaciones_cerradas()[0]["pnl_realizado"] == 9.6


def test_paper_registra_perdida_realizada():
    bot = PaperBot()
    bot.evaluar({
        "signal_id": "s1", "simbolo": "TEST", "accion": "BUY",
        "precio": 10.0, "stop_loss": 9.5,
    })
    d = bot.evaluar({
        "signal_id": "s2", "simbolo": "TEST", "accion": "EXIT",
        "precio": 9.5,
    })
    assert d["pnl_realizado"] == -6.0
    assert bot.status()["pnl_realizado_hoy"] == -6.0


def test_paper_bloquea_al_alcanzar_perdida_diaria_de_18_dolares():
    bot = PaperBot()

    for i, symbol in enumerate(("AAA", "BBB", "CCC"), start=1):
        bot.evaluar({
            "signal_id": f"buy{i}", "simbolo": symbol, "accion": "BUY",
            "precio": 10.0, "stop_loss": 9.5,
        })
        bot.evaluar({
            "signal_id": f"exit{i}", "simbolo": symbol, "accion": "EXIT",
            "precio": 9.5,
        })

    assert bot.status()["pnl_realizado_hoy"] == -18.0
    assert bot.status()["perdida_diaria_disponible"] == 0.0

    blocked = bot.evaluar({
        "signal_id": "buy4", "simbolo": "DDD", "accion": "BUY",
        "precio": 10.0, "stop_loss": 9.5,
    })
    assert blocked["action"] == "blocked"
    assert blocked["reason"] == "daily_loss_limit"


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
