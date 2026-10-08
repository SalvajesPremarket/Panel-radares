from BotTradeScanner.integracion.bot_long_realtime import BotLongRealtime
from BotTradeScanner.riesgo.paper import PaperBot, RiskConfig


class FakeBridge:
    def __init__(self, snapshot):
        self.snapshot_data = snapshot
        self.synced = []

    def sync_results(self, rows):
        self.synced = list(rows)

    def snapshot(self, symbol):
        data = dict(self.snapshot_data)
        data["simbolo"] = symbol
        return data


class SequenceBridge(FakeBridge):
    def __init__(self, snapshots):
        super().__init__(snapshots[0])
        self.snapshots = list(snapshots)
        self.index = 0

    def snapshot(self, symbol):
        data = dict(self.snapshots[min(self.index, len(self.snapshots) - 1)])
        self.index += 1
        data["simbolo"] = symbol
        return data


def valid_candidate_snapshot(**overrides):
    data = {
        "vela_actual": {
            "apertura": 10.0, "cierre": 10.1, "minimo": 10.0, "maximo": 10.2,
            "es_positiva": True, "regreso_a_apertura": False,
            "es_libelula_en_curso": False, "es_lapida_en_curso": False,
        },
        "ema20": 9.5, "ema20_anterior": 9.5,
        "ema50": 6.5, "ema50_anterior": 6.5,
        "ema200": 6.0, "ema200_anterior": 6.0,
        "macd": 0.5,
        "banda_bollinger_superior": 12.0, "banda_bollinger_superior_anterior": 12.0,
        "banda_bollinger_inferior": 5.0, "banda_bollinger_inferior_anterior": 5.0,
        "tramo_actual": 1,
        "ask": 10.1, "bid": 10.0,
    }
    data.update(overrides)
    return data


def test_normalized_signal_metadata_reaches_bot_decision():
    bridge = FakeBridge({"sin_datos": True})
    bot = BotLongRealtime(bridge)
    bot.sync_signals([{"symbol": "TEST", "signal_id": "sig-123", "confidence": 92,
                       "signal_type": "LONG", "timeframe": "1m"}])
    decisions = bot.evaluar_ahora()
    assert bridge.synced == [{"ticker": "TEST"}]
    assert decisions[0]["signal_id"] == "sig-123"
    assert decisions[0]["confidence"] == 92
    assert decisions[0]["signal_type"] == "LONG"
    assert decisions[0]["timeframe"] == "1m"


def test_long_signal_never_becomes_buy_without_strategy_confirmation():
    bridge = FakeBridge(valid_candidate_snapshot())
    bot = BotLongRealtime(bridge)
    bot.sync_signals([{"symbol": "TEST", "signal_id": "sig-456", "confidence": 97,
                       "signal_type": "LONG", "timeframe": "1m"}])
    decisions = bot.evaluar_ahora()
    assert decisions[0]["accion"] in {"WATCH", "WAIT"}
    assert decisions[0]["accion"] != "BUY"


def test_full_long_cycle_candidate_watch_buy_paper_and_stop_exit():
    snapshots = [
        valid_candidate_snapshot(),
        valid_candidate_snapshot(
            vela_actual={
                "apertura": 10.0, "cierre": 10.1, "minimo": 9.8, "maximo": 10.1,
                "es_positiva": True, "regreso_a_apertura": True,
                "es_libelula_en_curso": True, "es_lapida_en_curso": False,
            }
        ),
        valid_candidate_snapshot(
            vela_actual={
                "apertura": 10.1, "cierre": 9.9, "minimo": 9.8, "maximo": 10.1,
                "es_positiva": False, "regreso_a_apertura": False,
                "es_libelula_en_curso": False, "es_lapida_en_curso": False,
            }
        ),
    ]
    bridge = SequenceBridge(snapshots)
    bot = BotLongRealtime(bridge)
    bot.sync_signals([{"symbol": "TEST", "signal_id": "sig-cycle",
                       "confidence": 94, "signal_type": "LONG", "timeframe": "1m"}])

    first = bot.evaluar_ahora()
    second = bot.evaluar_ahora()
    third = bot.evaluar_ahora()

    assert first[0]["accion"] == "WATCH"
    assert second[0]["accion"] == "BUY"
    assert second[0]["paper"]["action"] == "buy"
    assert second[0]["paper"]["position_id"]
    assert third[0]["accion"] == "EXIT"
    assert third[0]["paper"]["action"] == "sell"
    assert third[0]["paper"]["pnl_realizado"] < 0
    assert bot.paper.status()["posiciones_abiertas"] == 0
    assert bot.paper.status()["operaciones_cerradas"] == 1


def test_paperbot_buy_then_exit_records_trade():
    paper = PaperBot()
    opened = paper.evaluar({"signal_id": "sig-paper", "simbolo": "TEST", "accion": "BUY",
                            "precio": 10.0, "stop_loss": 9.9,
                            "estrategia": "PreMarketSalvajes LONG"})
    assert opened["action"] == "buy"
    assert opened["position_id"]
    assert opened["capital_expuesto"] <= 120.0
    assert opened["riesgo_dolares"] <= 6.0
    closed = paper.evaluar({"signal_id": "sig-paper-exit", "simbolo": "TEST", "accion": "EXIT",
                            "precio": 10.2, "motivo": "test_exit"})
    assert closed["action"] == "sell"
    assert closed["pnl_realizado"] == 2.4
    assert paper.status()["posiciones_abiertas"] == 0
    assert paper.status()["operaciones_cerradas"] == 1


def test_paperbot_blocks_buy_without_stop():
    paper = PaperBot()
    result = paper.evaluar({"signal_id": "sig-no-stop", "simbolo": "TEST",
                             "accion": "BUY", "precio": 10.0})
    assert result["action"] == "blocked"
    assert result["reason"] == "missing_long_stop"
    assert paper.status()["posiciones_abiertas"] == 0


def test_paperbot_kill_switch_blocks_buy():
    paper = PaperBot(RiskConfig(kill_switch=True))
    result = paper.evaluar({"signal_id": "sig-kill", "simbolo": "TEST",
                             "accion": "BUY", "precio": 10.0, "stop_loss": 9.9})
    assert result["action"] == "blocked"
    assert result["reason"] == "kill_switch"
    assert paper.status()["posiciones_abiertas"] == 0


def test_paperbot_blocks_fourth_position():
    paper = PaperBot()
    for i, symbol in enumerate(("AAA", "BBB", "CCC", "DDD")):
        result = paper.evaluar({"signal_id": f"sig-{i}", "simbolo": symbol,
                                 "accion": "BUY", "precio": 10.0, "stop_loss": 9.9})
        if i < 3:
            assert result["action"] == "buy"
        else:
            assert result["action"] == "blocked"
            assert result["reason"] == "max_simultaneous_positions"
    assert paper.status()["posiciones_abiertas"] == 3


def test_paperbot_blocks_after_daily_loss_limit():
    paper = PaperBot()
    paper.evaluar({"signal_id": "sig-loss", "simbolo": "LOSS", "accion": "BUY",
                   "precio": 10.0, "stop_loss": 9.9})
    closed = paper.evaluar({"signal_id": "sig-loss-exit", "simbolo": "LOSS", "accion": "EXIT",
                            "precio": 7.0, "motivo": "daily_test_loss"})
    assert closed["pnl_realizado"] == -36.0
    blocked = paper.evaluar({"signal_id": "sig-after-loss", "simbolo": "NEXT", "accion": "BUY",
                             "precio": 10.0, "stop_loss": 9.9})
    assert blocked["action"] == "blocked"
    assert blocked["reason"] == "daily_loss_limit"
    assert paper.status()["posiciones_abiertas"] == 0
