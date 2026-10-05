from BotTradeScanner.integracion.bot_long_realtime import BotLongRealtime


def test_realtime_conserva_posicion_fuera_del_radar_y_la_cierra():
    class FakeBridge:
        def __init__(self):
            self.observados = []
            self.snaps = {}
        def sync_results(self, rows):
            self.observados.append({r["ticker"] for r in rows})
        def snapshot(self, simbolo):
            return self.snaps[simbolo]

    def s(**kw):
        base = {
            "simbolo": "TEST", "sin_datos": False, "tramo_actual": 1,
            "vela_actual": {"apertura": 10.0, "maximo": 10.0, "minimo": 9.7,
                            "cierre": 10.0, "es_positiva": False,
                            "es_libelula_en_curso": False,
                            "es_lapida_en_curso": False,
                            "regreso_a_apertura": False},
            "vela_anterior": {"minimo": 9.0, "maximo": 10.5, "cierre": 10.2},
            "minimo_supera_anterior": True, "maximo_supera_anterior": False,
            "ema9": 10.1, "ema20": 9.8, "ema50": 7.5, "ema200": 6.5,
            "ema9_anterior": 10.1, "ema20_anterior": 9.8,
            "ema50_anterior": 7.5, "ema200_anterior": 6.5,
            "macd": 0.2, "macd_anterior": 0.2,
            "banda_bollinger_superior": 12.0,
            "banda_bollinger_superior_anterior": 12.0,
            "banda_bollinger_inferior": 8.0,
            "banda_bollinger_inferior_anterior": 8.0,
        }
        for k,v in kw.items():
            if k == "vela_actual":
                base["vela_actual"].update(v)
            else:
                base[k] = v
        return base

    bridge = FakeBridge()
    bot = BotLongRealtime(bridge)

    bridge.snaps["TEST"] = s()
    bot.sync_candidates([{"ticker": "TEST"}])
    bot.evaluar_ahora()

    bridge.snaps["TEST"] = s(vela_actual={
        "minimo": 9.7, "cierre": 10.0, "es_libelula_en_curso": True
    })
    bot.evaluar_ahora()
    assert bot.paper.posiciones()

    bot.sync_candidates([])
    assert "TEST" in bridge.observados[-1]

    bridge.snaps["TEST"] = s(tramo_actual=3, vela_actual={
        "maximo": 12.2, "minimo": 11.8, "cierre": 11.8,
        "es_lapida_en_curso": True, "es_positiva": False
    })
    bot.evaluar_ahora()

    assert bot.paper.posiciones() == []
    assert bot.paper.operaciones_cerradas()
