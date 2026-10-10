import sqlite3

from webapp import storage
from webapp.server import startup


def test_startup_initializes_account_recommendations_schema(tmp_path, monkeypatch):
    database_path = tmp_path / "tradescanner.sqlite3"
    monkeypatch.setattr(storage, "SQLITE_PATH", database_path)
    monkeypatch.setattr(storage, "DATABASE_URL", "")

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
