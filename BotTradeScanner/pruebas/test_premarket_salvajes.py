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
        "ema9": 10.1, "ema20": 9.8, "ema50": 9.5, "ema200": 8.5,
        "ema9_anterior": 10.1, "ema20_anterior": 9.8, "ema50_anterior": 9.5, "ema200_anterior": 8.5,
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
    d = s.evaluar(snap(tramo=3, apertura=10, maximo=10.4, minimo=9.7, cierre=10, positiva=False, regreso=True))
    assert d.accion == "EXIT"


def test_bollinger_touch_without_gravestone_does_not_exit():
    s = PreMarketSalvajesLong()
    s.evaluar(snap(tramo=1, apertura=10, maximo=10, minimo=9.7, cierre=10, libelula=True))
    s.evaluar(snap(tramo=3, apertura=10, maximo=10.4, minimo=9.7, cierre=10.3, positiva=True))
    s.evaluar(snap(tramo=1, apertura=10.3, maximo=10.5, minimo=10.0, cierre=10.4, positiva=True, anterior_min=9.9))
    d = s.evaluar(snap(tramo=3, apertura=10.4, maximo=12.1, minimo=10.3, cierre=11.9, positiva=True, anterior_min=10.0, anterior_max=10.5))
    assert d.accion in {"HOLD", "WAIT"}
