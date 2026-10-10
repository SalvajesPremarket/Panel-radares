from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_landing_links_to_own_scanner_and_tradebot_pages():
    landing = (ROOT / "webapp" / "index.html").read_text(encoding="utf-8")

    assert 'href="/scanner"' in landing
    assert 'href="/tradebot"' in landing
    assert "radial-gradient" in landing
    # The landing page should send visitors to the platform's own scanner,
    # not make the legacy Streamlit app the primary scanner destination.
    assert 'href="https://jd6gih.streamlit.app"' not in landing


def test_scanner_dashboard_links_to_tradebot_and_shows_signal_adapter_status():
    scanner = (ROOT / "webapp" / "scanner" / "index.html").read_text(encoding="utf-8")

    assert 'href="/tradebot"' in scanner
    assert 'href="/"' in scanner
    assert "/api/v1/scanner/status" in scanner
    assert "/api/v1/signals?limit=100" in scanner
    assert "Clave receptora API" in scanner
    assert 'href="https://jd6gih.streamlit.app"' not in scanner


def test_tradebot_dashboard_keeps_paper_only_controls_and_own_scanner_link():
    tradebot = (ROOT / "webapp" / "tradebot" / "index.html").read_text(encoding="utf-8")

    assert 'href="/scanner"' in tradebot
    assert 'href="/"' in tradebot
    assert "/api/v1/tradebot/status" in tradebot
    assert "/api/v1/tradebot/start" in tradebot
    assert "/api/v1/tradebot/stop" in tradebot
    assert "real_trading_enabled" in tradebot
    assert "DESACTIVADAS" in tradebot
