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
2. Register or choose a domain, then create DNS A records for `WEB_DOMAIN` and `SCANNER_DOMAIN` pointing to the VPS public IP.
3. Open only SSH (preferably restricted to your IP), HTTP 80, and HTTPS 443 in the server firewall.
4. Install Docker Engine and the Docker Compose plugin.
5. Copy `.env.example` to `.env`; set real domains, a unique PostgreSQL password and two different random secrets. Generate hex-only values to avoid URL-escaping issues, for example `openssl rand -hex 24` for the DB password and `openssl rand -hex 32` for each secret. Never commit `.env`.
6. Add the required Alpaca market-data credentials only when ready to test data access in Paper. The automatic Paper runtime remains disabled by default.
7. From the repository root, run:

   ```bash
   docker compose --env-file .env -f deploy/self-host/compose.yaml config
   docker compose --env-file .env -f deploy/self-host/compose.yaml up -d --build
   docker compose --env-file .env -f deploy/self-host/compose.yaml ps
   docker compose --env-file .env -f deploy/self-host/compose.yaml logs --tail=100
   ```

Caddy can issue HTTPS certificates after both DNS records resolve publicly and ports 80/443 are reachable.

## Important pre-production checks

This setup is not yet a declaration that the platform is production-ready:

- The current signal adapter in `webapp/api/signal_service.py` stores signals in process memory, so a web restart loses that signal history. Move signals to PostgreSQL before relying on persistent group history.
- Paper positions and runtime state are also held in process memory. Define and test persistence/recovery before treating simulated performance records as durable.
- Verify the real end-to-end route: scanner final result → authenticated signal ingest → selected TradeBot strategy → Paper decision/exit.
- Verify account access, trial status, session cookies over HTTPS, restart behavior, database backup and restore, and memory usage with the chosen asset universe.
- Keep `TRADESCANNER_TRADEBOT_AUTO_PAPER=false` unless automatic Paper startup is deliberately being tested. No live order executor should be enabled as part of this migration.
- This Compose setup uses one VPS for cost efficiency; it is a single point of failure. Keep tested backups off the VPS before onboarding users.

## Backups and updates

Back up PostgreSQL regularly and copy backups to storage outside this VPS. Test restoring one backup before onboarding users. For updates, pull the reviewed Git commit and rebuild; do not edit application files directly on the server.

## Expected budget

Plan around the previously discussed USD 25–30/month for a 4 GB VPS plus a domain amortized monthly. Verify current provider prices, tax, domain renewal, and backup-storage charges before purchasing. This estimate is not a quote.
