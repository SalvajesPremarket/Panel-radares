#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT/deploy/self-host/.env}"

fail() {
  echo "Preflight error: $*" >&2
  exit 1
}

if [[ ! -f "$ENV_FILE" ]]; then
  fail "Missing $ENV_FILE. Copy .env.example to .env and configure it first."
fi

# The file is owner-managed configuration, never a repository secret.
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

for name in WEB_DOMAIN SCANNER_DOMAIN TRADESCANNER_COOKIE_DOMAIN POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD TRADESCANNER_SESSION_SECRET TRADESCANNER_SIGNAL_INGEST_SECRET; do
  value="${!name:-}"
  [[ -n "$value" ]] || fail "$name must be set in .env."
  case "$value" in
    *REPLACE*|*CHANGE_ME*|*example.com*|*yourdomain.com*)
      fail "$name still contains a placeholder value."
      ;;
  esac
done

# Keep secrets hex-only: PostgreSQL credentials are embedded in DATABASE_URL,
# and shell-safe values avoid accidental URL/shell escaping errors.
[[ "$POSTGRES_PASSWORD" =~ ^[[:xdigit:]]{48,}$ ]] ||
  fail "POSTGRES_PASSWORD must be at least 48 hexadecimal characters (openssl rand -hex 24)."
[[ "$TRADESCANNER_SESSION_SECRET" =~ ^[[:xdigit:]]{64,}$ ]] ||
  fail "TRADESCANNER_SESSION_SECRET must be at least 64 hexadecimal characters (openssl rand -hex 32)."
[[ "$TRADESCANNER_SIGNAL_INGEST_SECRET" =~ ^[[:xdigit:]]{64,}$ ]] ||
  fail "TRADESCANNER_SIGNAL_INGEST_SECRET must be at least 64 hexadecimal characters (openssl rand -hex 32)."

[[ "$POSTGRES_PASSWORD" != "$TRADESCANNER_SESSION_SECRET" ]] ||
  fail "Database password and session secret must be different."
[[ "$POSTGRES_PASSWORD" != "$TRADESCANNER_SIGNAL_INGEST_SECRET" ]] ||
  fail "Database password and ingest secret must be different."
[[ "$TRADESCANNER_SESSION_SECRET" != "$TRADESCANNER_SIGNAL_INGEST_SECRET" ]] ||
  fail "Session secret and ingest secret must be different."

# Refuse readable .env files; they contain credentials and session secrets.
mode="$(stat -c '%a' "$ENV_FILE")" || fail "Could not inspect .env file permissions."
mode_num=$((8#$mode))
(( (mode_num & 077) == 0 )) ||
  fail ".env permissions are too broad (mode $mode); run chmod 600 deploy/self-host/.env."

python3 - "$WEB_DOMAIN" "$SCANNER_DOMAIN" "$TRADESCANNER_COOKIE_DOMAIN" <<'PY'
import re
import sys

web, scanner, cookie = (value.strip().lower() for value in sys.argv[1:])
cookie = cookie.lstrip(".")
label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
hostname = re.compile(rf"(?=^.{{1,253}}$){label}(?:\.{label})+$")

for name, value in (("WEB_DOMAIN", web), ("SCANNER_DOMAIN", scanner), ("TRADESCANNER_COOKIE_DOMAIN", cookie)):
    if not hostname.fullmatch(value):
        raise SystemExit(f"Preflight error: {name} must be a real hostname without scheme, path, or port.")

if web == scanner:
    raise SystemExit("Preflight error: WEB_DOMAIN and SCANNER_DOMAIN must be different.")
if web == cookie or scanner == cookie or not web.endswith("." + cookie) or not scanner.endswith("." + cookie):
    raise SystemExit("Preflight error: both app domains must be subdomains of TRADESCANNER_COOKIE_DOMAIN.")
PY

echo "Preflight OK: required settings, independent secrets, domain/cookie alignment, and .env permissions look valid."
echo "This does not verify DNS, TLS issuance, provider credentials, live market data, or Paper behavior."
