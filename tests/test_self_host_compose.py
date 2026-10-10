from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_self_host_scanner_ingest_uses_private_web_service():
    compose = (ROOT / "deploy" / "self-host" / "compose.yaml").read_text(encoding="utf-8")

    assert "TRADESCANNER_SIGNAL_INGEST_URL: http://web:8000/api/v1/signals/ingest" in compose
    assert 'TRADESCANNER_TRADEBOT_AUTO_PAPER: "false"' in compose
    # Internal application and database ports must never be published on the VPS host.
    assert '"5432:5432"' not in compose
    assert '"8000:8000"' not in compose
    assert '"8501:8501"' not in compose
