# TradeScanner + TradeBot — self-hosted deployment

This is deployment scaffolding only. It does not create a server, buy a domain, or enable live trading. The intended first paid date is **20 December 2026**; keep all paid resources off until the owner approves that step.

## Layout

- `web`: FastAPI commercial web interface, account/session routes, and Paper TradeBot runtime.
- `scanner`: existing Streamlit TradeScanner app.
- `db`: PostgreSQL for the account/session storage already supported by `webapp/storage.py`.
- `proxy`: Caddy reverse proxy with automatic HTTPS for the two subdomains.

The app and scanner run as separate containers on one VPS. Only Caddy publishes ports 80 and 443; PostgreSQL and application ports stay on the private Docker network.

## Before first paid deployment

1. Choose a VPS provider and a plan with about 4 GB RAM, 2 vCPU, and 80 GB SSD, after checking current prices.
2. Register or choose one domain, then create DNS A records for `WEB_DOMAIN` and `SCANNER_DOMAIN` pointing to the VPS public IP. Use two subdomains under the same parent domain so the secure login cookie can be shared.
3. Open only SSH (preferably restricted to your IP), HTTP 80, and HTTPS 443 in the server firewall.
4. Install Docker Engine and the Docker Compose plugin.
5. From the repository root, copy `deploy/self-host/.env.example` to `deploy/self-host/.env`; set the real domains, `TRADESCANNER_COOKIE_DOMAIN` to their shared parent (for example `.yourdomain.com`), a unique PostgreSQL password and two different random secrets. Generate hex-only values to avoid URL-escaping issues, for example `openssl rand -hex 24` for the DB password and `openssl rand -hex 32` for each secret. Never commit `.env`.
6. Add the required Alpaca market-data credentials only when ready to test data access in Paper. The automatic Paper runtime remains disabled by default.
7. From the repository root, run:

   ```bash
   docker compose --env-file deploy/self-host/.env -f deploy/self-host/compose.yaml config
   docker compose --env-file deploy/self-host/.env -f deploy/self-host/compose.yaml up -d --build
   docker compose --env-file deploy/self-host/.env -f deploy/self-host/compose.yaml ps
   docker compose --env-file deploy/self-host/.env -f deploy/self-host/compose.yaml logs --tail=100
   ```

Caddy can issue HTTPS certificates after both DNS records resolve publicly and ports 80/443 are reachable. The scanner subdomain is protected by Caddy `forward_auth` against the web API; the session cookie must use the shared parent domain, and expired/suspended accounts must not be able to reach the scanner.

## Important pre-production checks

This setup is not yet a declaration that the platform is production-ready:

- Scanner signals and the manual Paper simulator's positions/decisions are persisted in PostgreSQL. The automatic LONG and SHORT Paper strategy states, including open simulated positions and decision history, are persisted and restored after restart. These recovery paths still require the full integration test with live market data before relying on unattended operation.
- The scanner publisher forwards EMA20 plus daily/weekly EMA50/EMA200 context when the motor has those values; delivery failures remain retryable. Verify the real end-to-end route: scanner final result → authenticated signal ingest → selected TradeBot strategy → Paper decision/exit.
- Verify that the scanner subdomain rejects requests without a valid session, allows an authorized trial/active/admin account, and rejects expired or suspended accounts. Also verify cookie sharing over HTTPS, restart behavior, database backup and restore, and memory use with the chosen asset universe.
- Keep `TRADESCANNER_TRADEBOT_AUTO_PAPER=false` unless automatic Paper startup is deliberately being tested. No live order executor should be enabled as part of this migration.
- This Compose setup uses one VPS for cost efficiency; it is a single point of failure. Keep tested backups off the VPS before onboarding users.

## Backups and updates

The repository includes `backup-postgres.sh`. It writes a compressed PostgreSQL dump outside the Git repository, checks the gzip file, and refuses to store backups under the project directory. Prefer a mounted remote/off-host destination so a VPS disk failure does not destroy both the database and its backup.

Run a manual backup from the repository root after configuring the real `.env`:

```bash
BACKUP_DIR=/mnt/offsite/tradescanner bash deploy/self-host/backup-postgres.sh
```

Replace `/mnt/offsite/tradescanner` with a mounted, protected backup destination. For daily automation, add a host cron entry only after the destination is mounted and writable; monitor failures.

To verify a backup, set `BACKUP_FILE` to the dump and `RESTORE_DATABASE` to a new database name (not the production database). The script refuses to overwrite an existing database and removes its newly created test database if the restore fails:

```bash
BACKUP_FILE=/mnt/offsite/tradescanner/tradescanner-postgres-YYYYMMDDTHHMMSSZ.sql.gz \
RESTORE_DATABASE=tradescanner_restore_test \
bash deploy/self-host/restore-postgres-test.sh
```

Inspect the restored tables/data, then remove the test database manually when finished. Backups contain user/account and trading-simulation history, so restrict access to the directory.

For updates, pull a reviewed Git commit and rebuild; do not edit application files directly on the server.

## Expected budget

Plan around the previously discussed USD 25–30/month for a 4 GB VPS plus a domain amortized monthly. Verify current provider prices, tax, domain renewal, and backup-storage charges before purchasing. This estimate is not a quote.
