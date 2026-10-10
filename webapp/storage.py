"""Database storage for TradeScanner.

Uses SQLite locally and Render Postgres when DATABASE_URL is configured.
On Render, PostgreSQL is mandatory: the local filesystem is not a safe place
for account/session data because it can be replaced during deploys.
"""
import os
import sqlite3
from pathlib import Path

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ROOT = Path(__file__).resolve().parent
SQLITE_PATH = Path(os.getenv("TRADESCANNER_DB", ROOT / "data" / "tradescanner.sqlite3"))


class Connection:
    def __init__(self, raw, postgres=False):
        self.raw = raw
        self.postgres = postgres

    def _sql(self, sql):
        return sql.replace("?", "%s") if self.postgres else sql

    def execute(self, sql, params=()):
        return self.raw.execute(self._sql(sql), params)

    def executemany(self, sql, params):
        return self.raw.executemany(self._sql(sql), params)

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.raw.rollback()

    def close(self):
        self.raw.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()


def db():
    if DATABASE_URL:
        import psycopg
        from psycopg.rows import dict_row
        return Connection(
            psycopg.connect(DATABASE_URL, row_factory=dict_row),
            postgres=True,
        )

    if os.getenv("RENDER", "").strip().lower() == "true":
        raise RuntimeError(
            "DATABASE_URL is required on Render; refusing to store account data "
            "in the ephemeral local filesystem."
        )

    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    return Connection(
        sqlite3.connect(SQLITE_PATH),
        postgres=False,
    )
