import pytest
from fastapi import HTTPException

from webapp import storage
from webapp.api import server, signal_service


def test_signal_ingest_secret_is_required_and_compared(monkeypatch):
    monkeypatch.delenv("TRADESCANNER_SIGNAL_INGEST_SECRET", raising=False)

    with pytest.raises(HTTPException) as missing:
        server.require_ingest_key(None)
    assert missing.value.status_code == 503

    monkeypatch.setenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "shared-test-secret")

    with pytest.raises(HTTPException) as invalid:
        server.require_ingest_key("wrong-secret")
    assert invalid.value.status_code == 401

    assert server.require_ingest_key("shared-test-secret") is None


def test_authenticated_signal_ingest_persists_scanner_context(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "signal-ingest.sqlite3")
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.setattr(signal_service, "_schema_identity", None)

    payload = server.SignalBatchIn(items=[
        server.SignalIn(
            symbol="aapl",
            timeframe="1m",
            signal_type="SCANNER_FINAL",
            price=12.5,
            confidence=82,
            scanner_conditions={"gap_pct": 4.2, "ema50_dia": 11.8},
        )
    ])

    result = server.ingest_signals(payload, None)
    saved = signal_service.store.list(symbol="AAPL", limit=10)

    assert result["accepted"] == 1
    assert len(result["signal_ids"]) == 1
    assert len(saved) == 1
    assert saved[0]["signal_id"] == result["signal_ids"][0]
    assert saved[0]["symbol"] == "AAPL"
    assert saved[0]["scanner_conditions"] == {
        "gap_pct": 4.2,
        "ema50_dia": 11.8,
    }

def test_scanner_publisher_to_authenticated_api_persists_final_signal(tmp_path, monkeypatch):
    from webapp.api import publisher

    monkeypatch.setattr(storage, "SQLITE_PATH", tmp_path / "scanner-to-api.sqlite3")
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.setattr(signal_service, "_schema_identity", None)
    monkeypatch.setenv("TRADESCANNER_SIGNAL_INGEST_URL", "https://self-host.test/api/v1/signals/ingest")
    monkeypatch.setenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "pipeline-test-secret")
    publisher._last_sent.clear()

    class FakeResponse:
        def raise_for_status(self):
            return None

    def fake_post(url, *, json, headers, timeout):
        assert url == "https://self-host.test/api/v1/signals/ingest"
        assert timeout == 3
        server.require_ingest_key(headers.get("X-TradeScanner-Signal-Key"))
        payload = server.SignalBatchIn(**json)
        result = server.ingest_signals(payload, None)
        assert result["accepted"] == 1
        return FakeResponse()

    monkeypatch.setattr(publisher._session, "post", fake_post)

    sent = publisher.publish_final_signals(
        [{"ticker": "AAPL", "precio": 12.5, "gap_pct": 4.2}],
        "1m",
    )
    saved = signal_service.store.list(symbol="AAPL", limit=10)

    assert sent == 1
    assert len(saved) == 1
    assert saved[0]["signal_type"] == "SCANNER_FINAL"
    assert saved[0]["price"] == 12.5
    assert saved[0]["scanner_conditions"]["gap_pct"] == 4.2

