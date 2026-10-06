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

    bars20 = [bar for bar in bars if bar.start_ts == 120]
    bars60 = [bar for bar in bars if bar.start_ts == 120]
    assert len(bars20) == 1
    assert len(bars60) == 1
    assert bars20[0].open == 10
    assert bars20[0].high == 12
    assert bars20[0].close == 12
    assert bars20[0].volume == 5
    assert bars20[0].trades == 2


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
    assert stream.health_snapshot()["errors"] == 0
