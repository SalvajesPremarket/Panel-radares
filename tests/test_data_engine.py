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
