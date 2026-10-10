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

# Load only the local, owner-managed .env. Keep secrets out of GitHub.
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

: "${POSTGRES_USER:?POSTGRES_USER must be set in .env}"
: "${POSTGRES_DB:?POSTGRES_DB must be set in .env}"
: "${BACKUP_DIR:?Set BACKUP_DIR to a backup directory outside the Git repository (preferably off-host or a mounted remote location)}"

mkdir -p "$BACKUP_DIR"
BACKUP_DIR="$(cd "$BACKUP_DIR" && pwd -P)"
case "$BACKUP_DIR/" in
  "$ROOT/"*)
    echo "Refusing to place database backups inside the Git repository." >&2
    exit 1
    ;;
esac

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
TARGET="$BACKUP_DIR/tradescanner-postgres-$STAMP.sql.gz"
TEMP="$TARGET.tmp"
trap 'rm -f "$TEMP"' EXIT

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T db \
  pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-owner --no-privileges \
  | gzip -9 > "$TEMP"

gzip -t "$TEMP"
test -s "$TEMP"
mv "$TEMP" "$TARGET"
trap - EXIT
echo "PostgreSQL backup created: $TARGET"
