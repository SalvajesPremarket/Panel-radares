import sqlite3

import pytest

from webapp import storage
from webapp.server import startup


def test_startup_initializes_account_recommendations_schema(tmp_path, monkeypatch):
    database_path = tmp_path / "tradescanner.sqlite3"
    monkeypatch.setattr(storage, "SQLITE_PATH", database_path)
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.delenv("RENDER", raising=False)

    startup()

    with sqlite3.connect(database_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            )
        }

    assert {"users", "sessions", "recommendations"} <= tables
    assert "idx_recommendations_user" in indexes


def test_render_requires_persistent_database_url(monkeypatch):
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.setenv("RENDER", "true")

    with pytest.raises(RuntimeError, match="DATABASE_URL is required on Render"):
        storage.db()
