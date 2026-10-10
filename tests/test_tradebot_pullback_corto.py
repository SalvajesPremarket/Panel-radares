from webapp.tradebot.pullback_corto import PullbackCortoEMA, STRATEGY_NAME


def snapshot(*, price=9.79, open_price=9.8, high=11.0, low=9.75, tramo=1,
             previous=None, age=0.1):
    return {
        "simbolo": "XYZ",
        "tramo_actual": tramo,
        "market_data_trade_age_sec": age,
        "ema20": 9.7,
        "vela_actual": {
            "apertura": open_price,
            "maximo": high,
            "minimo": low,
            "cierre": price,
        },
        "vela_anterior": previous or {
            "apertura": 10.0,
            "maximo": 12.0,
            "minimo": 9.9,
            "cierre": 9.95,
        },
    }


def conditions():
    return {"ema50_daily": 10.5, "ema20": 9.7}


def test_missing_daily_weekly_ema_context_blocks_entry():
    engine = PullbackCortoEMA()
    result = engine.evaluate(snapshot(), {"ema20": 9.7})
    assert result["accion"] == "WAIT"
    assert result["estado"] == "falta_contexto_ema"
    assert result["position_open"] is False


def test_second_gravestone_with_lower_high_opens_paper_short():
    engine = PullbackCortoEMA()
    result = engine.evaluate(snapshot(), conditions())
    assert result["accion"] == "SHORT"
    assert result["estrategia"] == STRATEGY_NAME
    assert result["stop_loss"] == 9.95
    assert result["position_open"] is True


def test_short_exits_immediately_if_first_20_seconds_cross_open():
    engine = PullbackCortoEMA()
    opened = engine.evaluate(snapshot(), conditions())
    assert opened["accion"] == "SHORT"

    current = snapshot(price=9.81, open_price=9.8, tramo=1)
    result = engine.evaluate(current, conditions())
    assert result["accion"] == "EXIT"
    assert result["estado"] == "salida_tercio_1"
    assert result["position_open"] is False


def test_stale_market_data_blocks_entry():
    engine = PullbackCortoEMA()
    result = engine.evaluate(snapshot(age=10), conditions())
    assert result["accion"] == "WAIT"
    assert result["estado"] == "feed_obsoleto"
