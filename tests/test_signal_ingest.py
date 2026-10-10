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
