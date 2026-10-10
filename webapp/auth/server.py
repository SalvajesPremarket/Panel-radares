import base64
import calendar
import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi import Cookie, FastAPI, HTTPException, Response
from pydantic import BaseModel, EmailStr, Field

from webapp.storage import db

SESSION_DAYS = int(os.getenv("TRADESCANNER_SESSION_DAYS", "30"))
COOKIE_NAME = "tradescanner_session"


def cookie_domain():
    """Optional parent domain so the same secure session covers web and scanner subdomains."""
    value = os.getenv("TRADESCANNER_COOKIE_DOMAIN", "").strip()
    return value or None

app = FastAPI(title="TradeScanner Auth API", version="0.1.0")


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    display_name: str | None = Field(default=None, max_length=100)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=128)


def utcnow():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def add_one_month(dt):
    year, month = dt.year, dt.month
    if month == 12:
        year, month = year + 1, 1
    else:
        month += 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


def startup():
    with db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            role TEXT NOT NULL DEFAULT 'user',
            account_status TEXT NOT NULL DEFAULT 'trial',
            trial_started_at TEXT NOT NULL,
            trial_ends_at TEXT NOT NULL,
            trial_used INTEGER NOT NULL DEFAULT 1,
            subscription_plan TEXT,
            subscription_status TEXT,
            created_at TEXT NOT NULL,
            last_login_at TEXT
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            token_hash TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            revoked_at TEXT
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS sessions_user_id_idx ON sessions(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS sessions_expires_at_idx ON sessions(expires_at)")


def password_hash(password):
    salt = secrets.token_bytes(16)
    rounds = 310000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, rounds)
    return "pbkdf2_sha256$" + str(rounds) + "$" + base64.urlsafe_b64encode(salt).decode() + "$" + base64.urlsafe_b64encode(digest).decode()


def password_verify(password, encoded):
    try:
        algorithm, rounds, salt_b64, digest_b64 = encoded.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        salt = base64.urlsafe_b64decode(salt_b64.encode())
        expected = base64.urlsafe_b64decode(digest_b64.encode())
        actual = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, int(rounds))
        return hmac.compare_digest(actual, expected)
    except (ValueError, TypeError):
        return False


def token_hash(token):
    secret = os.getenv("TRADESCANNER_SESSION_SECRET", "")
    if not secret:
        raise RuntimeError("TRADESCANNER_SESSION_SECRET is required")
    return hmac.new(secret.encode(), token.encode(), hashlib.sha256).hexdigest()


def create_session(user_id):
    token = secrets.token_urlsafe(48)
    now = utcnow()
    expires = now + timedelta(days=SESSION_DAYS)
    with db() as conn:
        conn.execute(
            "INSERT INTO sessions(session_id,user_id,token_hash,created_at,expires_at) VALUES(?,?,?,?,?)",
            (str(uuid4()), user_id, token_hash(token), iso(now), iso(expires)),
        )
    return token


def get_current_user(tradescanner_session: str | None = Cookie(default=None)):
    return current_user(tradescanner_session)


def current_user(session_token):
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    with db() as conn:
        row = conn.execute("""SELECT u.* FROM users u
            JOIN sessions s ON s.user_id = u.user_id
            WHERE s.token_hash = ? AND s.revoked_at IS NULL AND s.expires_at > ?""",
            (token_hash(session_token), iso(utcnow()))).fetchone()
    if not row:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return row


@app.get("/health")
def health():
    return {"ok": True, "service": "tradescanner-auth"}


@app.post("/auth/register")
def register(payload: RegisterRequest, response: Response):
    email = payload.email.lower().strip()
    now = utcnow()
    trial_end = add_one_month(now)
    user_id = str(uuid4())
    with db() as conn:
        if conn.execute("SELECT user_id FROM users WHERE email = ?", (email,)).fetchone():
            raise HTTPException(status_code=409, detail="Email already registered")
        conn.execute("""INSERT INTO users
            (user_id,email,password_hash,display_name,role,account_status,trial_started_at,trial_ends_at,trial_used,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (user_id, email, password_hash(payload.password), payload.display_name, "user", "trial", iso(now), iso(trial_end), 1, iso(now)))
    token = create_session(user_id)
    response.set_cookie(COOKIE_NAME, token, max_age=SESSION_DAYS * 86400,
                        httponly=True, secure=True, samesite="lax", path="/",
                        domain=cookie_domain())
    return {"user_id": user_id, "account_status": "trial", "trial_ends_at": iso(trial_end)}


@app.post("/auth/login")
def login(payload: LoginRequest, response: Response):
    email = payload.email.lower().strip()
    with db() as conn:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    if not row or not password_verify(payload.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if row["account_status"] == "suspended":
        raise HTTPException(status_code=403, detail="Account suspended")
    now = utcnow()
    with db() as conn:
        conn.execute("UPDATE users SET last_login_at = ? WHERE user_id = ?", (iso(now), row["user_id"]))
    token = create_session(row["user_id"])
    response.set_cookie(COOKIE_NAME, token, max_age=SESSION_DAYS * 86400,
                        httponly=True, secure=True, samesite="lax", path="/",
                        domain=cookie_domain())
    return {"user_id": row["user_id"], "account_status": row["account_status"]}


@app.post("/auth/logout")
def logout(response: Response, tradescanner_session: str | None = Cookie(default=None)):
    if tradescanner_session:
        with db() as conn:
            conn.execute("UPDATE sessions SET revoked_at = ? WHERE token_hash = ?",
                         (iso(utcnow()), token_hash(tradescanner_session)))
    response.delete_cookie(COOKIE_NAME, path="/", domain=cookie_domain())
    return {"ok": True}


@app.get("/auth/me")
def me(tradescanner_session: str | None = Cookie(default=None)):
    row = current_user(tradescanner_session)
    return {"user_id": row["user_id"], "email": row["email"], "display_name": row["display_name"],
            "role": row["role"], "account_status": row["account_status"],
            "trial_started_at": row["trial_started_at"], "trial_ends_at": row["trial_ends_at"],
            "subscription_plan": row["subscription_plan"], "subscription_status": row["subscription_status"]}
