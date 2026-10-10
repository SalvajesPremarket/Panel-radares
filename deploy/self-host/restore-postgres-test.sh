#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="$ROOT/deploy/self-host/.env"
COMPOSE_FILE="$ROOT/deploy/self-host/compose.yaml"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE. Copy .env.example to .env and configure it first." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

: "${POSTGRES_USER:?POSTGRES_USER must be set in .env}"
: "${POSTGRES_DB:?POSTGRES_DB must be set in .env}"
: "${BACKUP_FILE:?Set BACKUP_FILE to an existing compressed PostgreSQL dump}"
: "${RESTORE_DATABASE:?Set RESTORE_DATABASE to a new, separate test database name}"

if [[ ! -f "$BACKUP_FILE" ]]; then
  echo "Backup file does not exist: $BACKUP_FILE" >&2
  exit 1
fi
gzip -t "$BACKUP_FILE"

if [[ ! "$RESTORE_DATABASE" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "RESTORE_DATABASE may contain only letters, numbers, and underscores." >&2
  exit 1
fi
if [[ "$RESTORE_DATABASE" == "$POSTGRES_DB" ]]; then
  echo "Refusing to restore into the production database." >&2
  exit 1
fi

COMPOSE=(docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE")
EXISTS="$("${COMPOSE[@]}" exec -T db psql -U "$POSTGRES_USER" -d postgres -Atqc "SELECT 1 FROM pg_database WHERE datname = '$RESTORE_DATABASE'")"
if [[ "$EXISTS" == "1" ]]; then
  echo "Refusing to overwrite existing database: $RESTORE_DATABASE" >&2
  exit 1
fi

"${COMPOSE[@]}" exec -T db createdb -U "$POSTGRES_USER" "$RESTORE_DATABASE"
RESTORE_OK=0
cleanup() {
  if [[ "$RESTORE_OK" != "1" ]]; then
    "${COMPOSE[@]}" exec -T db dropdb --if-exists -U "$POSTGRES_USER" "$RESTORE_DATABASE" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

gzip -dc "$BACKUP_FILE" | "${COMPOSE[@]}" exec -T db psql -U "$POSTGRES_USER" -d "$RESTORE_DATABASE" -v ON_ERROR_STOP=1
TABLE_COUNT="$("${COMPOSE[@]}" exec -T db psql -U "$POSTGRES_USER" -d "$RESTORE_DATABASE" -Atqc "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")"
if [[ ! "$TABLE_COUNT" =~ ^[0-9]+$ ]] || (( TABLE_COUNT < 1 )); then
  echo "Restore did not produce any public tables; removing the test database." >&2
  exit 1
fi

RESTORE_OK=1
trap - EXIT
echo "Restore verified in separate database '$RESTORE_DATABASE' ($TABLE_COUNT public tables)."
echo "Inspect the test database, then remove it manually when finished."
