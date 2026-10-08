from BotTradeScanner.integracion.bot_long_realtime import BotLongRealtime
from BotTradeScanner.riesgo.paper import PaperBot


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


def valid_candidate_snapshot(**overrides):
    data = {
        "vela_actual": {
            "apertura": 10.0,
            "cierre": 10.1,
            "minimo": 10.0,
            "maximo": 10.2,
            "es_positiva": True,
            "regreso_a_apertura": False,
            "es_libelula_en_curso": False,
            "es_lapida_en_curso": False,
        },
        "ema20": 9.5,
        "ema20_anterior": 9.5,
        "ema50": 9.0,
        "ema50_anterior": 9.0,
        "ema200": 8.0,
        "ema200_anterior": 8.0,
        "macd": 0.5,
        "banda_bollinger_superior": 12.0,
        "banda_bollinger_superior_anterior": 12.0,
        "banda_bollinger_inferior": 7.0,
        "banda_bollinger_inferior_anterior": 7.0,
        "tramo_actual": 1,
    }
    data.update(overrides)
    return data


def test_normalized_signal_metadata_reaches_bot_decision():
    bridge = FakeBridge({"sin_datos": True})
    bot = BotLongRealtime(bridge)
    bot.sync_signals([{
        "symbol": "TEST",
        "signal_id": "sig-123",
        "confidence": 92,
        "signal_type": "LONG",
        "timeframe": "1m",
    }])

    decisions = bot.evaluar_ahora()

    assert bridge.synced == [{"ticker": "TEST"}]
    assert decisions[0]["signal_id"] == "sig-123"
    assert decisions[0]["confidence"] == 92
    assert decisions[0]["signal_type"] == "LONG"
    assert decisions[0]["timeframe"] == "1m"


def test_long_signal_never_becomes_buy_without_strategy_confirmation():
    bridge = FakeBridge(valid_candidate_snapshot())
    bot = BotLongRealtime(bridge)
    bot.sync_signals([{
        "symbol": "TEST",
        "signal_id": "sig-456",
        "confidence": 97,
        "signal_type": "LONG",
        "timeframe": "1m",
    }])

    decisions = bot.evaluar_ahora()

    assert decisions[0]["accion"] in {"WATCH", "WAIT"}
    assert decisions[0]["accion"] != "BUY"


def test_paperbot_buy_then_exit_records_trade():
    paper = PaperBot()
    opened = paper.evaluar({
        "signal_id": "sig-paper",
        "simbolo": "TEST",
        "accion": "BUY",
        "precio": 10.0,
        "stop_loss": 9.9,
        "estrategia": "PreMarketSalvajes LONG",
    })

    assert opened["action"] == "buy"
    assert opened["position_id"]

    closed = paper.evaluar({
        "signal_id": "sig-paper-exit",
        "simbolo": "TEST",
        "accion": "EXIT",
        "precio": 10.2,
        "motivo": "test_exit",
    })

    assert closed["action"] == "sell"
    assert closed["pnl_realizado"] == 2.4
    assert paper.status()["posiciones_abiertas"] == 0
    assert paper.status()["operaciones_cerradas"] == 1
