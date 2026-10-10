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

    assert "TRADESCANNER_COOKIE_DOMAIN: ${TRADESCANNER_COOKIE_DOMAIN:?Set TRADESCANNER_COOKIE_DOMAIN in .env}" in compose

    caddy = (ROOT / "deploy" / "self-host" / "Caddyfile").read_text(encoding="utf-8")
    assert "forward_auth web:8000" in caddy
    assert "uri /api/v1/scanner/status" in caddy

    env_example = (ROOT / "deploy" / "self-host" / ".env.example").read_text(encoding="utf-8")
    assert "TRADESCANNER_COOKIE_DOMAIN=.example.com" in env_example


def test_auth_cookie_domain_can_cover_web_and_scanner(monkeypatch):
    from webapp.auth.server import cookie_domain

    monkeypatch.setenv("TRADESCANNER_COOKIE_DOMAIN", ".yourdomain.com")
    assert cookie_domain() == ".yourdomain.com"

    monkeypatch.delenv("TRADESCANNER_COOKIE_DOMAIN", raising=False)
    assert cookie_domain() is None
