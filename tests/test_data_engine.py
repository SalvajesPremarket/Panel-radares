from datetime import datetime, timezone

from TradeScanner.data_engine import DataHealth, LiveBarBuilder, MarketCache


def test_market_cache_keeps_latest_quote_and_trade():
    cache = MarketCache()
    cache.update_quote("aapl", 100, 100.25, "q1")
    cache.update_trade("aapl", 100.2, 5, "t1")

    assert cache.quote("AAPL")["ask"] == 100.25
    assert cache.trade("AAPL")["price"] == 100.2
    assert cache.symbols() == {"AAPL"}


def test_live_bar_builder_builds_20s_and_60s_bars():
    builder = LiveBarBuilder()
    ts = datetime.fromtimestamp(120.5, tz=timezone.utc)

    builder.on_trade("AAPL", 10, 2, ts)
    bars = builder.on_trade("AAPL", 12, 3, datetime.fromtimestamp(139.9, tz=timezone.utc))

    assert len(bars) == 2
    bar20 = builder.get("AAPL", 20)
    bar60 = builder.get("AAPL", 60)
    assert bar20 is not None
    assert bar60 is not None
    for bar in (bar20, bar60):
        assert bar.start_ts == 120
        assert bar.open == 10
        assert bar.high == 12
        assert bar.close == 12
        assert bar.volume == 5
        assert bar.trades == 2


def test_data_health_reports_live_events():
    health = DataHealth()
    health.start("iex")
    health.mark_quote("AAPL")
    health.mark_trade("AAPL")

    snap = health.snapshot()
    assert snap["connected"] is True
    assert snap["feed"] == "iex"
    assert snap["quotes"] == 1
    assert snap["trades"] == 1
    assert snap["symbols_seen"] == 1


def test_alpaca_market_stream_start_update_and_stop(monkeypatch):
    import threading
    import time

    import TradeScanner.data_engine.market_stream as module

    class FakeStream:
        instances = []

        def __init__(self, *args, **kwargs):
            self.quotes = set()
            self.trades = set()
            self.stopped = False
            self.ready = threading.Event()
            FakeStream.instances.append(self)

        def subscribe_quotes(self, callback, *symbols):
            self.quotes.update(symbols)

        def subscribe_trades(self, callback, *symbols):
            self.trades.update(symbols)

        def unsubscribe_quotes(self, *symbols):
            self.quotes.difference_update(symbols)

        def unsubscribe_trades(self, *symbols):
            self.trades.difference_update(symbols)

        def run(self):
            self.ready.set()
            while not self.stopped:
                time.sleep(0.01)

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(module, "StockDataStream", FakeStream)

    stream = module.AlpacaMarketStream("key", "secret", feed="iex")
    stream.start(["aapl", "msft"])

    deadline = time.time() + 2
    while not FakeStream.instances and time.time() < deadline:
        time.sleep(0.01)

    assert FakeStream.instances
    fake = FakeStream.instances[0]
    assert fake.ready.wait(timeout=2)
    assert fake.quotes == {"AAPL", "MSFT"}
    assert fake.trades == {"AAPL", "MSFT"}

    stream.update_symbols(["MSFT", "NVDA"])
    assert fake.quotes == {"MSFT", "NVDA"}
    assert fake.trades == {"MSFT", "NVDA"}

    stream.stop()
    assert fake.stopped is True
    assert stream.health_snapshot()["feed"] == "iex"
    assert stream.health_snapshot()["errors"] == 0


def test_alpaca_market_stream_empty_update_during_connect_does_not_leave_stale_symbols(monkeypatch):
    import threading
    import time

    import TradeScanner.data_engine.market_stream as module

    class FakeStream:
        instances = []

        def __init__(self, *args, **kwargs):
            self.quotes = set()
            self.trades = set()
            self.stopped = False
            self.entered_subscribe = threading.Event()
            self.allow_subscribe = threading.Event()
            FakeStream.instances.append(self)

        def subscribe_quotes(self, callback, *symbols):
            self.entered_subscribe.set()
            assert self.allow_subscribe.wait(timeout=3)
            self.quotes.update(symbols)

        def subscribe_trades(self, callback, *symbols):
            self.trades.update(symbols)

        def unsubscribe_quotes(self, *symbols):
            self.quotes.difference_update(symbols)

        def unsubscribe_trades(self, *symbols):
            self.trades.difference_update(symbols)

        def run(self):
            while not self.stopped:
                time.sleep(0.01)

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(module, "StockDataStream", FakeStream)
    stream = module.AlpacaMarketStream("key", "secret", feed="iex")
    stream.start(["AAPL"])
    deadline = time.time() + 2
    while not FakeStream.instances and time.time() < deadline:
        time.sleep(0.01)
    assert FakeStream.instances
    fake = FakeStream.instances[0]
    assert fake.entered_subscribe.wait(timeout=2)

    update_done = threading.Event()
    updater = threading.Thread(target=lambda: (stream.start([]), update_done.set()))
    updater.start()
    deadline = time.time() + 2
    while stream._symbols and time.time() < deadline:
        time.sleep(0.01)
    assert stream._symbols == set()

    fake.allow_subscribe.set()
    assert update_done.wait(timeout=2)
    deadline = time.time() + 2
    while (fake.quotes or fake.trades) and time.time() < deadline:
        time.sleep(0.01)

    assert fake.quotes == set()
    assert fake.trades == set()
    assert stream.health_snapshot()["subscribed_symbols"] == []
    stream.stop()


def test_alpaca_market_stream_reconnects_after_run_error(monkeypatch):
    import threading
    import time

    import TradeScanner.data_engine.market_stream as module

    class FakeStream:
        instances = []

        def __init__(self, *args, **kwargs):
            self.stopped = False
            self.ready = threading.Event()
            FakeStream.instances.append(self)

        def subscribe_quotes(self, callback, *symbols):
            pass

        def subscribe_trades(self, callback, *symbols):
            pass

        def run(self):
            self.ready.set()
            if len(FakeStream.instances) == 1:
                raise RuntimeError("simulated disconnect")
            while not self.stopped:
                time.sleep(0.01)

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(module, "StockDataStream", FakeStream)
    stream = module.AlpacaMarketStream("key", "secret", feed="iex")
    stream.start(["AAPL"])

    deadline = time.time() + 4
    while len(FakeStream.instances) < 2 and time.time() < deadline:
        time.sleep(0.02)

    assert len(FakeStream.instances) >= 2
    assert FakeStream.instances[1].ready.wait(timeout=2)
    assert stream._stream is FakeStream.instances[1]
    assert FakeStream.instances[0].stopped is True
    assert stream.health_snapshot()["running"] is True
    stream.stop()


def test_shared_stream_trade_reaches_motor_velas_end_to_end():
    import asyncio
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from BotTradeScanner.motor_velas.motor_velas import MotorVelas
    from TradeScanner.data_engine import AlpacaMarketStream

    stream = AlpacaMarketStream("key", "secret", feed="iex")
    motor = MotorVelas("key", "secret")
    motor.conectar_stream_compartido(stream)

    trade = SimpleNamespace(
        symbol="AAPL",
        price=10.25,
        size=50,
        timestamp=datetime(2026, 10, 9, 16, 0, 15, tzinfo=timezone.utc),
    )
    asyncio.run(stream._trade(trade))

    assert stream.health_snapshot()["trades"] == 1
    assert motor.total_trades == 1
    assert motor.ultimo_trade == trade.timestamp
    assert "AAPL" in motor.motores
    assert motor.motores["AAPL"].vela_actual.cierre == 10.25
    assert motor.motores["AAPL"].vela_actual.volumen == 50


def test_shared_stream_consumers_receive_trades_and_quotes():
    import asyncio
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from TradeScanner.data_engine import AlpacaMarketStream

    stream = AlpacaMarketStream("key", "secret", feed="iex")
    received_trades = []
    received_quotes = []
    stream.add_consumer(received_trades.append, received_quotes.append)

    trade = SimpleNamespace(
        symbol="AAPL", price=10.5, size=100,
        timestamp=datetime(2026, 10, 9, 16, 0, tzinfo=timezone.utc),
    )
    quote = SimpleNamespace(
        symbol="AAPL", bid_price=10.4, ask_price=10.6,
        timestamp=datetime(2026, 10, 9, 16, 0, tzinfo=timezone.utc),
    )
    asyncio.run(stream._trade(trade))
    asyncio.run(stream._quote(quote))

    assert received_trades == [trade]
    assert received_quotes == [quote]
    snap = stream.health_snapshot()
    assert snap["trades"] == 1
    assert snap["quotes"] == 1
    assert snap["last_event_kind"] == "quote"
    assert snap["last_event_symbol"] == "AAPL"


def test_motor_velas_receives_trade_from_shared_alpaca_stream():
    import asyncio
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from BotTradeScanner.motor_velas.motor_velas import MotorVelas
    from TradeScanner.data_engine import AlpacaMarketStream

    stream = AlpacaMarketStream("key", "secret", feed="iex")
    motor = MotorVelas("key", "secret")
    motor.conectar_stream_compartido(stream)

    trade = SimpleNamespace(
        symbol="AAPL",
        price=10.5,
        size=100,
        timestamp=datetime(2026, 10, 9, 16, 0, tzinfo=timezone.utc),
    )
    quote = SimpleNamespace(
        symbol="AAPL",
        bid_price=10.4,
        ask_price=10.6,
        timestamp=datetime(2026, 10, 9, 16, 0, tzinfo=timezone.utc),
    )
    asyncio.run(stream._trade(trade))
    asyncio.run(stream._quote(quote))

    snapshot = motor.snapshot_simbolo("AAPL")
    assert motor.total_trades == 1
    assert snapshot["vela_actual"]["cierre"] == 10.5
    assert snapshot["bid"] == 10.4
    assert snapshot["ask"] == 10.6
    assert stream.health_snapshot()["trade_consumer_errors"] == 0


def test_alpaca_market_stream_preserves_candidate_priority_order():
    from TradeScanner.data_engine import AlpacaMarketStream

    stream = AlpacaMarketStream("key", "secret", feed="iex", max_symbols=3)
    assert stream._normalizar_simbolos(
        ["TSLA", "AAPL", "MSFT", "TSLA", "NVDA"]
    ) == ["TSLA", "AAPL", "MSFT"]


def test_alpaca_market_stream_empty_start_does_not_open_connection(monkeypatch):
    import time

    import TradeScanner.data_engine.market_stream as module

    class FakeStream:
        instances = []

        def __init__(self, *args, **kwargs):
            FakeStream.instances.append(self)

        def run(self):
            raise AssertionError("empty start must not open a websocket")

    monkeypatch.setattr(module, "StockDataStream", FakeStream)
    stream = module.AlpacaMarketStream("key", "secret", feed="iex")
    stream.start([])
    time.sleep(0.05)

    assert FakeStream.instances == []
    assert stream._thread is None
    assert stream.health_snapshot()["errors"] == 0


def test_alpaca_market_stream_empty_update_unsubscribes_previous_symbols(monkeypatch):
    import threading
    import time

    import TradeScanner.data_engine.market_stream as module

    class FakeStream:
        instances = []

        def __init__(self, *args, **kwargs):
            self.quotes = set()
            self.trades = set()
            self.stopped = False
            self.ready = threading.Event()
            FakeStream.instances.append(self)

        def subscribe_quotes(self, callback, *symbols):
            self.quotes.update(symbols)

        def subscribe_trades(self, callback, *symbols):
            self.trades.update(symbols)

        def unsubscribe_quotes(self, *symbols):
            self.quotes.difference_update(symbols)

        def unsubscribe_trades(self, *symbols):
            self.trades.difference_update(symbols)

        def run(self):
            self.ready.set()
            while not self.stopped:
                time.sleep(0.01)

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(module, "StockDataStream", FakeStream)
    stream = module.AlpacaMarketStream("key", "secret", feed="iex")
    stream.start(["AAPL"])
    deadline = time.time() + 2
    while not FakeStream.instances and time.time() < deadline:
        time.sleep(0.01)
    assert FakeStream.instances
    fake = FakeStream.instances[0]
    assert fake.ready.wait(timeout=2)

    stream.start([])
    assert fake.quotes == set()
    assert fake.trades == set()
    assert stream._symbols == set()
    stream.stop()

def test_motor_velas_bridge_enforces_stream_symbol_budget_and_cleans_old_symbols():
    import threading

    from BotTradeScanner.integracion.live_motor_bridge import MotorVelasBridge

    class FakeStream:
        def __init__(self):
            self.last_symbols = None

        def start(self, symbols):
            self.last_symbols = list(symbols)

    class FakeMotor:
        def __init__(self):
            self.removed = []
            self.removed_event = threading.Event()

        def conectar_stream_compartido(self, stream):
            pass

        def quitar_simbolo_en_caliente(self, symbol):
            self.removed.append(symbol)
            if len(self.removed) >= 3:
                self.removed_event.set()

    stream = FakeStream()
    motor = FakeMotor()
    bridge = MotorVelasBridge("key", "secret", motor=motor, market_stream=stream)
    bridge.MAX_SIMBOLOS_BASIC = 7
    symbols = ["TSLA", "AAPL", "MSFT", "NVDA", "AMD", "META", "PLTR", "AMZN", "GOOG", "INTC"]
    bridge._simbolos_cargados = set(symbols)
    bridge.sync_results([{"ticker": symbol} for symbol in symbols])

    assert stream.last_symbols == symbols[:7]
    assert bridge._simbolos_deseados == set(symbols[:7])
    assert bridge._simbolos_cargados == set(symbols[:7])
    assert motor.removed_event.wait(timeout=2)
    assert set(motor.removed) == set(symbols[7:])



def test_motor_velas_removes_stale_symbol_history_and_quotes():
    from BotTradeScanner.motor_velas.motor_velas import MotorVelas

    motor = MotorVelas("key", "secret")
    motor._obtener_motor("AAPL")
    motor._historial_precargado.add("AAPL")
    motor._quotes["AAPL"] = {"bid": 10.0, "ask": 10.1}

    motor.quitar_simbolo_en_caliente("AAPL")

    assert "AAPL" not in motor.motores
    assert "AAPL" not in motor._historial_precargado
    assert "AAPL" not in motor._quotes



def test_health_snapshot_is_safe_during_subscription_updates():
    import threading

    from TradeScanner.data_engine import AlpacaMarketStream

    stream = AlpacaMarketStream("key", "secret", feed="iex")
    failures = []

    def mutate():
        for i in range(3000):
            with stream._subscription_lock:
                stream._subscribed_symbols.clear()
                stream._subscribed_symbols.update(f"S{i}_{j}" for j in range(i % 20))

    worker = threading.Thread(target=mutate)
    worker.start()
    for _ in range(3000):
        try:
            snapshot = stream.health_snapshot()
            assert isinstance(snapshot["subscribed_symbols"], list)
        except Exception as exc:
            failures.append(exc)
    worker.join(timeout=3)

    assert not worker.is_alive()
    assert failures == []



def test_alpaca_server_subscription_errors_are_exposed_in_health_snapshot():
    import asyncio

    from TradeScanner.data_engine import AlpacaMarketStream

    class FakeSDKStream:
        def __init__(self):
            self.received = []

        async def _dispatch(self, message):
            self.received.append(message)

    stream = AlpacaMarketStream("key", "secret", feed="iex")
    sdk_stream = FakeSDKStream()
    stream._instrument_stream_dispatch(sdk_stream)

    asyncio.run(sdk_stream._dispatch({
        "T": "error",
        "code": 405,
        "msg": "symbol limit exceeded",
    }))

    health = stream.health_snapshot()
    assert health["errors"] == 1
    assert "405" in health["last_error"]
    assert "symbol limit exceeded" in health["last_error"]
    assert sdk_stream.received[0]["code"] == 405
