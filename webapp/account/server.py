"""TradeScanner account API.
Authenticated account/access/recommendation endpoints. Does not import or modify app.py.
"""
from datetime import datetime, timezone
from uuid import uuid4
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel, Field
from webapp.auth.server import get_current_user, trial_has_expired
from webapp.storage import db

app = FastAPI(title="TradeScanner Account API", version="0.1.0")
ALLOWED_ACCOUNT_STATUSES = {"trial", "active_monthly", "active_annual", "admin"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def init_account_schema():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS recommendations (
            recommendation_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            category TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            symbol TEXT,
            context TEXT,
            status TEXT NOT NULL DEFAULT 'new',
            priority TEXT NOT NULL DEFAULT 'P2',
            admin_notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_recommendations_user ON recommendations(user_id, created_at DESC)")


class RecommendationIn(BaseModel):
    category: str = Field(min_length=1, max_length=40)
    title: str = Field(min_length=1, max_length=160)
    description: str = Field(min_length=1, max_length=4000)
    symbol: str | None = Field(default=None, max_length=20)
    context: str | None = Field(default=None, max_length=4000)


def account_user(user=Depends(get_current_user)):
    if user["account_status"] not in ALLOWED_ACCOUNT_STATUSES:
        raise HTTPException(status_code=403, detail="Acceso de cuenta no activo")
    if trial_has_expired(user):
        raise HTTPException(status_code=403, detail="El periodo de prueba ha vencido")
    return user


@app.get("/health")
def health():
    return {"ok": True, "service": "account"}


@app.get("/account/me")
def me(user=Depends(account_user)):
    return {k: user[k] for k in (
        "user_id", "email", "display_name", "role", "account_status", "trial_started_at",
        "trial_ends_at", "subscription_plan", "subscription_status", "created_at", "last_login_at")}


@app.get("/account/access")
def access(user=Depends(account_user)):
    allowed = user["account_status"] in ALLOWED_ACCOUNT_STATUSES
    return {"allowed": allowed, "account_status": user["account_status"],
            "scanner": allowed, "signals_api": allowed, "bot": allowed,
            "admin": user["role"] == "admin"}


@app.get("/account/recommendations")
def recommendations(user=Depends(account_user)):
    with db() as conn:
        rows = conn.execute("""SELECT recommendation_id,category,title,description,symbol,
            context,status,priority,admin_notes,created_at,updated_at
            FROM recommendations WHERE user_id=? ORDER BY created_at DESC""", (user["user_id"],)).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.post("/account/recommendations", status_code=201)
def create_recommendation(payload: RecommendationIn, user=Depends(account_user)):
    now = utc_now()
    rid = str(uuid4())
    with db() as conn:
        conn.execute("""INSERT INTO recommendations
            (recommendation_id,user_id,category,title,description,symbol,context,status,priority,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?, 'new','P2',?,?)""",
            (rid, user["user_id"], payload.category, payload.title, payload.description, payload.symbol, payload.context, now, now))
    return {"recommendation_id": rid, "status": "new", "priority": "P2", "created_at": now}
