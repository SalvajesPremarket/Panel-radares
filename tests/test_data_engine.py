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
