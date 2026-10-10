from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PREFLIGHT = REPO_ROOT / "deploy" / "self-host" / "preflight.sh"


def _fixture(tmp_path: Path, **overrides: str) -> tuple[Path, dict[str, str]]:
    root = tmp_path / "repo"
    target = root / "deploy" / "self-host"
    target.mkdir(parents=True)
    script = target / "preflight.sh"
    shutil.copy2(PREFLIGHT, script)
    env_file = target / ".env"
    values = {
        "WEB_DOMAIN": "app.trade.example.net",
        "SCANNER_DOMAIN": "scanner.trade.example.net",
        "TRADESCANNER_COOKIE_DOMAIN": ".trade.example.net",
        "POSTGRES_DB": "tradescanner",
        "POSTGRES_USER": "tradescanner",
        "POSTGRES_PASSWORD": "a" * 48,
        "TRADESCANNER_SESSION_SECRET": "b" * 64,
        "TRADESCANNER_SIGNAL_INGEST_SECRET": "c" * 64,
    }
    values.update(overrides)
    env_file.write_text("\n".join(f"{key}={value}" for key, value in values.items()) + "\n", encoding="utf-8")
    env_file.chmod(0o600)
    return script, {"ENV_FILE": str(env_file)}


def _run(script: Path, extra_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(extra_env)
    return subprocess.run(["bash", str(script)], text=True, capture_output=True, env=env, check=False)


def test_preflight_accepts_valid_self_host_configuration(tmp_path: Path) -> None:
    script, env = _fixture(tmp_path)
    result = _run(script, env)

    assert result.returncode == 0, result.stderr
    assert "Preflight OK" in result.stdout
    assert "b" * 64 not in result.stdout
    assert "c" * 64 not in result.stdout


def test_preflight_rejects_placeholder_secret(tmp_path: Path) -> None:
    script, env = _fixture(tmp_path, TRADESCANNER_SESSION_SECRET="REPLACE_WITH_RANDOM_SESSION_SECRET")
    result = _run(script, env)

    assert result.returncode != 0
    assert "placeholder" in result.stderr


def test_preflight_rejects_reused_secrets(tmp_path: Path) -> None:
    secret = "d" * 64
    script, env = _fixture(tmp_path, TRADESCANNER_SESSION_SECRET=secret, TRADESCANNER_SIGNAL_INGEST_SECRET=secret)
    result = _run(script, env)

    assert result.returncode != 0
    assert "must be different" in result.stderr


def test_preflight_rejects_cookie_domain_mismatch(tmp_path: Path) -> None:
    script, env = _fixture(tmp_path, TRADESCANNER_COOKIE_DOMAIN=".other.example.net")
    result = _run(script, env)

    assert result.returncode != 0
    assert "subdomains" in result.stderr


def test_preflight_rejects_world_readable_env_file(tmp_path: Path) -> None:
    script, env = _fixture(tmp_path)
    Path(env["ENV_FILE"]).chmod(0o644)
    result = _run(script, env)

    assert result.returncode != 0
    assert "permissions are too broad" in result.stderr
