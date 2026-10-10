import requests

from webapp.api import publisher


class FakeResponse:
    def __init__(self, error=False):
        self.error = error

    def raise_for_status(self):
        if self.error:
            raise requests.RequestException("temporary network error")


class FakeBridge:
    def snapshot(self, symbol):
        assert symbol == "AAPL"
        return {
            "ema20": 12.0,
            "ema50_diaria": 11.0,
            "ema200_diaria": 10.0,
            "ema50_semanal": 9.0,
            "ema200_semanal": 8.0,
        }


def test_publisher_includes_higher_timeframe_ema_context(monkeypatch):
    publisher._last_sent.clear()
    monkeypatch.setenv("TRADESCANNER_SIGNAL_INGEST_URL", "https://example.test/ingest")
    monkeypatch.setenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "test-secret")
    calls = []

    def post(*args, **kwargs):
        calls.append(kwargs)
        return FakeResponse()

    monkeypatch.setattr(publisher._session, "post", post)
    count = publisher.publish_final_signals(
        [{"ticker": "AAPL", "precio": 12.5, "tecnico_ema20": 12.1}],
        "1m",
        motor_bridge=FakeBridge(),
    )

    assert count == 1
    item = calls[0]["json"]["items"][0]
    assert item["scanner_conditions"]["ema20"] == 12.1
    assert item["scanner_conditions"]["ema50_dia"] == 11.0
    assert item["scanner_conditions"]["ema200_dia"] == 10.0
    assert item["scanner_conditions"]["ema50_semana"] == 9.0
    assert item["scanner_conditions"]["ema200_semana"] == 8.0
    assert calls[0]["headers"]["X-TradeScanner-Signal-Key"] == "test-secret"


def test_failed_signal_publish_is_retryable(monkeypatch):
    publisher._last_sent.clear()
    monkeypatch.setenv("TRADESCANNER_SIGNAL_INGEST_URL", "https://example.test/ingest")
    monkeypatch.setenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "test-secret")
    calls = []

    def post(*args, **kwargs):
        calls.append(kwargs)
        return FakeResponse(error=len(calls) == 1)

    monkeypatch.setattr(publisher._session, "post", post)
    signal = [{"ticker": "MSFT", "precio": 20.0}]
    assert publisher.publish_final_signals(signal, "1m") == 0
    assert publisher.publish_final_signals(signal, "1m") == 1
    assert len(calls) == 2
