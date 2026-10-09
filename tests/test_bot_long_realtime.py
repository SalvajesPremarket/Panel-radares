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
        "ema50": 4.5, "ema50_anterior": 4.5,
        "ema200": 4.0, "ema200_anterior": 4.0,
        "macd": 0.5,
        "banda_bollinger_superior": 12.0, "banda_bollinger_superior_anterior": 12.0,
        "banda_bollinger_inferior": 5.0, "banda_bollinger_inferior_anterior": 5.0,
        "tramo_actual": 1,
        "ask": 10.1, "bid": 10.0,
    }
    data.update(overrides)
    return data


def test_sync_signals_preserves_scanner_priority_order():
    bridge = FakeBridge({"sin_datos": True})
    bot = BotLongRealtime(bridge)
    bot.sync_signals([
        {"ticker": "ZZZ"},
        {"ticker": "AAA"},
        {"ticker": "MMM"},
        {"ticker": "AAA"},
    ])
    assert bridge.synced == [
        {"ticker": "ZZZ"},
        {"ticker": "AAA"},
        {"ticker": "MMM"},
    ]


def test_active_long_symbols_are_prioritized_before_new_candidates():
    from BotTradeScanner.estrategias.long.premarket_salvajes import (
        EstadoLong,
        PreMarketSalvajesLong,
    )

    bridge = FakeBridge({"sin_datos": True})
    bot = BotLongRealtime(bridge)
    active = PreMarketSalvajesLong()
    active.estado = EstadoLong.LONG_PRIMERA_VELA
    bot.decisiones._estrategias["ZZACTIVE"] = active

    bot.sync_signals([
        {"ticker": "ZZZ"},
        {"ticker": "AAA"},
        {"ticker": "MMM"},
        {"ticker": "BBB"},
        {"ticker": "CCC"},
        {"ticker": "DDD"},
        {"ticker": "EEE"},
        {"ticker": "FFF"},
    ])

    assert bridge.synced[0] == {"ticker": "ZZACTIVE"}
    assert bridge.synced[1:4] == [
        {"ticker": "ZZZ"},
        {"ticker": "AAA"},
        {"ticker": "MMM"},
    ]


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
                "apertura": 10.1, "cierre": 9.8, "minimo": 9.7, "maximo": 10.1,
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


def test_full_long_cycle_take_profit_closes_paper_position():
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
                "apertura": 10.5, "cierre": 10.6, "minimo": 10.4, "maximo": 10.6,
                "es_positiva": True, "regreso_a_apertura": False,
                "es_libelula_en_curso": False, "es_lapida_en_curso": False,
            }
        ),
    ]
    bridge = SequenceBridge(snapshots)
    bot = BotLongRealtime(bridge)
    bot.sync_signals([{"symbol": "TEST", "signal_id": "sig-tp",
                       "confidence": 94, "signal_type": "LONG", "timeframe": "1m"}])

    bot.evaluar_ahora()
    opened = bot.evaluar_ahora()
    closed = bot.evaluar_ahora()

    assert opened[0]["accion"] == "BUY"
    assert opened[0]["paper"]["action"] == "buy"
    assert closed[0]["accion"] == "EXIT"
    assert "Take Profit" in closed[0]["motivo"]
    assert closed[0]["paper"]["action"] == "sell"
    assert closed[0]["paper"]["pnl_realizado"] > 0
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


class FakeTradingClient:
    def __init__(self):
        self.submissions = 0
        self.cancels = 0

    def submit_order(self, order_data):
        self.submissions += 1
        return type("Order", (), {
            "client_order_id": order_data["client_order_id"] if isinstance(order_data, dict) else "cid-1",
            "id": "order-1",
            "status": "new",
            "filled_qty": 0,
            "filled_avg_price": None,
        })()

    def get_order_by_client_id(self, client_order_id):
        return type("Order", (), {
            "client_order_id": client_order_id,
            "id": "order-1",
            "status": "new",
            "filled_qty": 0,
            "filled_avg_price": None,
        })()

    def cancel_order_by_id(self, order_id):
        self.cancels += 1


def test_paper_execution_prepares_limit_at_ask_and_stays_disabled():
    from BotTradeScanner.ejecucion.alpaca import AlpacaExecutor
    from BotTradeScanner.ejecucion.configuracion import ExecutionConfig

    config = ExecutionConfig.por_defecto()
    executor = AlpacaExecutor(config)
    order = executor.preparar("TEST", ask=10.25, bid=10.20, cantidad=5, client_order_id="cid-disabled")
    assert order.tipo == "limit"
    assert order.limit_price == 10.25
    result = executor.enviar_buy(order)
    assert result.status == "disabled"
    assert result.enviada is False


def test_paper_execution_uses_client_order_id_idempotently(monkeypatch):
    from BotTradeScanner.ejecucion.alpaca import AlpacaExecutor
    from BotTradeScanner.ejecucion.configuracion import ExecutionConfig

    config = ExecutionConfig.por_defecto()
    config.enabled = True
    client = FakeTradingClient()
    executor = AlpacaExecutor(config, trading_client=client)
    monkeypatch.setattr(executor, "_build_request", lambda order: {"client_order_id": order.client_order_id})

    order = executor.preparar("TEST", ask=10.25, bid=10.20, cantidad=5, client_order_id="cid-idempotent")
    first = executor.enviar_buy(order)
    second = executor.enviar_buy(order)

    assert first.status == "new"
    assert second == first
    assert client.submissions == 1


def test_paper_execution_terminal_rejection_does_not_create_position():
    from BotTradeScanner.ejecucion.alpaca import AlpacaExecutor
    from BotTradeScanner.ejecucion.configuracion import ExecutionConfig

    config = ExecutionConfig.por_defecto()
    config.enabled = True
    client = FakeTradingClient()
    executor = AlpacaExecutor(config, trading_client=client)
    monkeypatch_result = type("Order", (), {
        "client_order_id": "cid-reject",
        "id": "order-reject",
        "status": "rejected",
        "filled_qty": 0,
        "filled_avg_price": None,
    })()
    monkeypatch = __import__("pytest").MonkeyPatch()
    monkeypatch.setattr(executor, "_build_request", lambda order: {"client_order_id": order.client_order_id})
    client.submit_order = lambda order_data: monkeypatch_result
    order = executor.preparar("TEST", ask=10.25, bid=10.20, cantidad=5, client_order_id="cid-reject")
    result = executor.enviar_buy(order)
    assert result.status == "rejected"
    assert result.filled_qty == 0
    monkeypatch.undo()

def test_status_identifica_paper_y_feed_compartido_sin_habilitar_broker():
    class Bridge:
        def sync_results(self, rows):
            pass

        def snapshot(self, simbolo):
            return {"simbolo": simbolo, "sin_datos": True}

        def status(self):
            return {
                "stream_compartido": True,
                "stream_connected": True,
                "subscribed_symbols": ["TEST"],
            }

    bot = BotLongRealtime(Bridge())
    bot.sync_candidates([{"ticker": "TEST", "precio": 10.25, "actualizado": "2026-10-09T14:30:00Z",
                         "ema20_estado": "arriba", "macd_positivo": True}])

    status = bot.status()
    assert status["modo_ejecucion"] == "paper_simulation"
    assert status["envio_broker_habilitado"] is False
    assert status["executor_configurado"] is False
    assert status["market_data"]["stream_compartido"] is True
    assert status["cantidad_candidatos"] == 1


def test_scanner_context_is_attached_to_bot_decision():
    bridge = FakeBridge(valid_candidate_snapshot())
    bot = BotLongRealtime(bridge)
    bot.sync_candidates([{
        "ticker": "TEST",
        "precio": 10.25,
        "actualizado": "2026-10-09T14:30:00Z",
        "ema20_estado": "arriba",
        "ema50_estado": "arriba",
        "ema200_estado": "abajo",
        "tecnico_ema20": 10.1,
        "macd_positivo": True,
        "macd_negativo": False,
        "gap_pct": 12.5,
        "volumen_dia": 250000,
        "float_shares": 1000000,
        "tecnico_barras": 5,
    }])

    decisions = bot.evaluar_ahora()
    assert decisions
    assert decisions[0]["scanner_price"] == 10.25
    assert decisions[0]["scanner_timestamp"] == "2026-10-09T14:30:00Z"
    assert decisions[0]["scanner_conditions"]["ema20_estado"] == "arriba"
    assert decisions[0]["scanner_conditions"]["macd_positivo"] is True
    assert decisions[0]["scanner_conditions"]["gap_pct"] == 12.5

def test_shared_live_feed_rejects_candidate_without_fresh_trade():
    class SharedBridge:
        def sync_results(self, rows):
            self.synced = list(rows)

        def snapshot(self, symbol):
            data = valid_candidate_snapshot()
            data["simbolo"] = symbol
            data["market_data_trade_age_sec"] = 45.0
            return data

        def status(self):
            return {"stream_compartido": True, "stream_connected": True}

    bridge = SharedBridge()
    bot = BotLongRealtime(bridge)
    bot.sync_candidates([{"ticker": "TEST", "precio": 10.25}])

    decisions = bot.evaluar_ahora()

    assert decisions == []
    assert bot.paper.status()["posiciones_abiertas"] == 0
    assert bot.status()["ultimo_error"] == "TEST: trade_live_obsoleto:45.0s"


def test_shared_live_feed_waits_for_first_trade_after_historical_preload():
    class SharedBridge:
        def sync_results(self, rows):
            self.synced = list(rows)

        def snapshot(self, symbol):
            data = valid_candidate_snapshot()
            data["simbolo"] = symbol
            data["market_data_trade_age_sec"] = None
            return data

        def status(self):
            return {"stream_compartido": True, "stream_connected": True}

    bot = BotLongRealtime(SharedBridge())
    bot.sync_candidates([{"ticker": "TEST", "precio": 10.25}])

    assert bot.evaluar_ahora() == []
    assert bot.paper.status()["posiciones_abiertas"] == 0
    assert bot.status()["ultimo_error"] == "TEST: trade_live_ausente_o_obsoleto"

def test_shared_live_feed_blocks_long_entry_when_quote_is_stale():
    snapshots = [
        valid_candidate_snapshot(market_data_trade_age_sec=1.0, market_data_quote_age_sec=45.0),
        valid_candidate_snapshot(
            vela_actual={
                "apertura": 10.0, "cierre": 10.1, "minimo": 9.8, "maximo": 10.1,
                "es_positiva": True, "regreso_a_apertura": True,
                "es_libelula_en_curso": True, "es_lapida_en_curso": False,
            },
            market_data_trade_age_sec=1.0,
            market_data_quote_age_sec=45.0,
        ),
    ]

    class SharedSequenceBridge(SequenceBridge):
        def status(self):
            return {"stream_compartido": True, "stream_connected": True}

    bot = BotLongRealtime(SharedSequenceBridge(snapshots))
    bot.sync_candidates([{"ticker": "TEST", "precio": 10.25}])

    bot.evaluar_ahora()
    decision = bot.evaluar_ahora()

    assert decision[0]["accion"] == "WAIT"
    assert decision[0]["motivo"] == "quote_live_obsoleta:45.0s"
    assert bot.paper.status()["posiciones_abiertas"] == 0

