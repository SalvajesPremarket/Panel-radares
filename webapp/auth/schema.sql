-- TradeScanner authentication schema
-- Reference schema. Production deployment must use parameterized migrations.

CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    display_name TEXT,
    role TEXT NOT NULL DEFAULT 'user',
    account_status TEXT NOT NULL DEFAULT 'trial',
    trial_started_at TIMESTAMPTZ NOT NULL,
    trial_ends_at TIMESTAMPTZ NOT NULL,
    trial_used BOOLEAN NOT NULL DEFAULT TRUE,
    subscription_plan TEXT,
    subscription_status TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ
);

CREATE TABLE sessions (
    session_id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);

CREATE INDEX sessions_user_id_idx ON sessions(user_id);
CREATE INDEX sessions_expires_at_idx ON sessions(expires_at);
