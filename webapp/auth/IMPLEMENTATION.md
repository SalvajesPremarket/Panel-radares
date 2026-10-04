# Authentication Implementation Order

## Step 1 — Data layer
Implement users and sessions with the schema in `schema.sql`.

## Step 2 — Registration
Create account, normalize email, hash password, calculate the one-month trial on the server, and create the first session.

## Step 3 — Login
Verify credentials, revoke or rotate stale sessions when appropriate, update last_login_at, and issue a new secure session.

## Step 4 — Middleware
Every protected request resolves the session server-side and then checks:
1. session validity;
2. account status;
3. role;
4. requested capability.

## Step 5 — Account panel
Show only non-sensitive information:
- email;
- plan;
- trial/subscription status;
- trial expiration;
- connection status;
- bot mode.

Never show API secrets.

## Step 6 — Recovery
Add email verification and password recovery before production launch.

## Step 7 — Testing
Automated tests must cover:
- duplicate registration;
- invalid credentials;
- expired session;
- revoked session;
- trial expiration;
- suspended user;
- admin access;
- normal user denied admin functions;
- refresh preserving session.

## Production gate
Do not connect real broker order execution until authentication, authorization, audit logging and paper trading are validated.
