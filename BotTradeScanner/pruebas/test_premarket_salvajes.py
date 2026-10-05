from BotTradeScanner.estrategias.long.premarket_salvajes import PreMarketSalvajesLong


def snap(tramo=1, apertura=10.0, maximo=10.0, minimo=10.0, cierre=10.0, positiva=False, libelula=False, lapida=False, regreso=False, anterior_min=9.0, anterior_max=10.5, anterior_cierre=10.2):
    return {
        "simbolo": "TEST", "sin_datos": False, "tramo_actual": tramo,
        "vela_actual": {
            "apertura": apertura, "maximo": maximo, "minimo": minimo, "cierre": cierre,
            "es_positiva": positiva, "es_libelula_en_curso": libelula,
            "es_lapida_en_curso": lapida, "regreso_a_apertura": regreso,
        },
        "vela_anterior": {"minimo": anterior_min, "maximo": anterior_max, "cierre": anterior_cierre},
        "minimo_supera_anterior": minimo > anterior_min,
        "maximo_supera_anterior": maximo > anterior_max,
        "ema9": 10.1, "ema20": 9.8, "ema50": 7.5, "ema200": 6.5,
        "ema9_anterior": 10.1, "ema20_anterior": 9.8, "ema50_anterior": 7.5, "ema200_anterior": 6.5,
        "macd": 0.2, "macd_anterior": 0.2,
        "banda_bollinger_superior": 12.0, "banda_bollinger_superior_anterior": 12.0,
        "banda_bollinger_inferior": 8.0, "banda_bollinger_inferior_anterior": 8.0,
    }


def test_entry_requires_candidate_then_dragonfly():
    s = PreMarketSalvajesLong()
    d = s.evaluar(snap(tramo=1, apertura=10, maximo=10, minimo=9.7, cierre=10, libelula=False))
    assert d.accion == "WATCH"
    d = s.evaluar(snap(tramo=1, apertura=10, maximo=10, minimo=9.7, cierre=10, libelula=True))
    assert d.accion == "BUY"


def test_first_candle_returns_to_open_in_third_segment_exits():
    s = PreMarketSalvajesLong()
    s.evaluar(snap(tramo=1, apertura=10, maximo=10, minimo=9.7, cierre=10, libelula=True))
    s.confirmar_fill(10.0)
    d = s.evaluar(snap(tramo=3, apertura=10, maximo=10.4, minimo=9.7, cierre=10, positiva=False, regreso=True))
    assert d.accion == "EXIT"


def test_bollinger_touch_without_gravestone_does_not_exit():
    s = PreMarketSalvajesLong()
    s.evaluar(snap(tramo=1, apertura=10, maximo=10, minimo=9.7, cierre=10, libelula=True))
    s.confirmar_fill(10.0)
    s.evaluar(snap(tramo=3, apertura=10, maximo=10.4, minimo=9.7, cierre=10.3, positiva=True))
    s.evaluar(snap(tramo=1, apertura=10.3, maximo=10.5, minimo=10.0, cierre=10.4, positiva=True, anterior_min=9.9))
    d = s.evaluar(snap(tramo=3, apertura=10.4, maximo=12.1, minimo=10.3, cierre=11.9, positiva=True, anterior_min=10.0, anterior_max=10.5))
    assert d.accion in {"HOLD", "WAIT"}


def test_full_long_sequence_moves_stop_and_exits_on_bollinger_gravestone():
    s = PreMarketSalvajesLong()

    # 1) Candidato valido + libelula -> BUY.
    d = s.evaluar(snap(
        tramo=1, apertura=10.0, maximo=10.0, minimo=9.7,
        cierre=10.0, libelula=True,
    ))
    assert d.accion == "BUY"
    assert d.estado.value == "orden_pendiente"
    assert d.stop_loss == 10.0
    s.confirmar_fill(10.01)

    # 2) Primera vela continua positiva en tramo 2.
    d = s.evaluar(snap(
        tramo=2, apertura=10.0, maximo=10.5, minimo=9.8,
        cierre=10.3, positiva=True,
    ))
    assert d.accion == "HOLD"
    assert d.estado.value == "long_primera_vela"
    assert d.stop_loss == 10.0

    # 3) Tramo 3 positivo/consolidando -> pasa a esperar segunda vela.
    d = s.evaluar(snap(
        tramo=3, apertura=10.0, maximo=10.7, minimo=9.9,
        cierre=10.6, positiva=True,
    ))
    assert d.accion == "HOLD"
    assert d.estado.value == "long_segunda_vela"

    # 4) Segunda vela: positiva + higher low. En tramo 1,
    #    stop = cierre de la vela de entrada.
    d = s.evaluar(snap(
        tramo=1, apertura=10.6, maximo=10.9, minimo=10.0,
        cierre=10.8, positiva=True,
        anterior_min=9.9, anterior_max=10.7, anterior_cierre=10.0,
    ))
    assert d.accion == "HOLD"
    assert d.estado.value == "long_segunda_vela"
    assert d.stop_loss == 10.0

    # 5) Segunda vela termina positiva: pasa a LONG_SIGUIENTES.
    d = s.evaluar(snap(
        tramo=3, apertura=10.6, maximo=11.0, minimo=10.1,
        cierre=10.9, positiva=True,
        anterior_min=9.9, anterior_max=10.7, anterior_cierre=10.0,
    ))
    assert d.accion == "HOLD"
    assert d.estado.value == "long_siguientes"

    # 6) Tercera vela positiva + higher low -> stop sube al cierre
    #    de la vela inmediatamente anterior.
    d = s.evaluar(snap(
        tramo=2, apertura=10.9, maximo=11.4, minimo=10.2,
        cierre=11.3, positiva=True,
        anterior_min=10.1, anterior_max=11.0, anterior_cierre=10.9,
    ))
    assert d.accion == "HOLD"
    assert d.stop_loss == 10.9

    # 7) Toca Bollinger superior pero sigue positiva: NO cerrar.
    d = s.evaluar(snap(
        tramo=3, apertura=11.3, maximo=12.1, minimo=11.2,
        cierre=11.9, positiva=True,
        anterior_min=10.2, anterior_max=11.4, anterior_cierre=11.3,
    ))
    assert d.accion == "HOLD"
    assert d.stop_loss == 11.3

    # 8) Bollinger + lapida -> EXIT.
    d = s.evaluar(snap(
        tramo=3, apertura=11.9, maximo=12.2, minimo=11.8,
        cierre=11.8, positiva=False, lapida=True,
        anterior_min=11.2, anterior_max=12.1, anterior_cierre=11.9,
    ))
    assert d.accion == "EXIT"
    assert d.estado.value == "pullback_long"


def test_stop_is_not_percentage_trailing_stop():
    s = PreMarketSalvajesLong()

    s.evaluar(snap(
        tramo=1, apertura=10.0, maximo=10.0, minimo=9.7,
        cierre=10.0, libelula=True,
    ))
    s.evaluar(snap(
        tramo=3, apertura=10.0, maximo=10.5, minimo=9.8,
        cierre=10.4, positiva=True,
    ))
    s.evaluar(snap(
        tramo=1, apertura=10.4, maximo=10.8, minimo=10.1,
        cierre=10.7, positiva=True, anterior_min=9.8,
    ))
    s.evaluar(snap(
        tramo=3, apertura=10.4, maximo=11.0, minimo=10.2,
        cierre=10.9, positiva=True, anterior_min=10.1,
        anterior_max=10.8, anterior_cierre=10.4,
    ))

    # Debe usar exactamente el cierre de la vela anterior (10.4),
    # no un trailing porcentual calculado desde 10.9.
    assert s.stop_loss == 10.4


def test_decision_machine_enforces_strategy_stop():
    from BotTradeScanner.decision.maquina_decisiones import MaquinaDecisionesLong

    m = MaquinaDecisionesLong()

    # Candidato -> libelula -> LONG.
    d = m.evaluar(snap(
        tramo=1, apertura=10.0, maximo=10.0, minimo=9.7,
        cierre=10.0, libelula=True,
    ))
    assert d.accion == "WATCH"

    d = m.evaluar(snap(
        tramo=1, apertura=10.0, maximo=10.0, minimo=9.7,
        cierre=10.0, libelula=True,
    ))
    assert d.accion == "BUY"

    # El precio cae al stop vigente de 10.0: la maquina debe producir EXIT
    # aunque la estrategia todavía no haya evaluado una regla de vela.
    d = m.evaluar(snap(
        tramo=2, apertura=10.0, maximo=10.2, minimo=9.9,
        cierre=10.0, positiva=False,
    ))
    assert d.accion == "EXIT"
    assert d.stop_loss == 10.0


def test_pullback_requires_ema_support_higher_low_and_higher_high():
    s = PreMarketSalvajesLong()
    s.marcar_salida_para_pullback()

    # Solo tocar EMA20 no basta.
    d = s.evaluar(snap(
        tramo=2, apertura=10.0, maximo=10.5, minimo=9.7,
        cierre=10.3, positiva=True,
        anterior_min=9.5, anterior_max=10.6,
    ))
    assert d.accion == "WAIT"
    assert d.estado.value == "pullback_long"

    # Soporte + higher low + higher high habilita vigilar una nueva entrada,
    # pero NO abre una segunda posicion automaticamente.
    d = s.evaluar(snap(
        tramo=2, apertura=10.0, maximo=10.8, minimo=10.0,
        cierre=10.6, positiva=True,
        anterior_min=9.9, anterior_max=10.7,
    ))
    assert d.accion == "WATCH"
    assert d.pullback_reentrada is True
    assert d.estado.value == "esperando_libelula"
