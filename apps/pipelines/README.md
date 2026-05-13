# pipelines

Prefect 3 ingestion flows for the unique-stocks data platform.

---

## Project structure

```text
apps/pipelines/
├── domains/
│   ├── eod_prices/      EOD OHLCV — daily (implemented)
│   ├── exchanges/       Exchange list — manual (stub)
│   ├── securities/      Securities per exchange — weekly (stub)
│   └── fundamentals/    Financials — quarterly (stub)
├── core/
│   ├── config.py        Pydantic Settings (reads .env)
│   ├── lake.py          DuckDB / MotherDuck helpers
│   ├── models.py        BronzeModel base class
│   ├── scheduler.py     NYSE calendar helpers
│   └── clients/
│       ├── base.py      BaseClient ABC
│       └── eodhd.py     EODHD API wrapper
└── tests/
    └── unit/
```

Each domain follows the same pattern: `models.py` → `transforms.py` → `tasks.py` → `flows.py`

---

## Local development (no Docker)

**Prerequisites:** Python 3.14+, [uv](https://docs.astral.sh/uv/)

```bash
cd apps/pipelines

# 1. Install deps
uv sync

# 2. Create .env
cp .env.example .env
# Set EODHD_API_KEY — leave MOTHERDUCK_TOKEN blank to use local DuckDB

# 3. Start a local Prefect server in a separate terminal
make prefect-server       # → UI at http://localhost:4200

# 4. Register work pool and deployments (once)
make prefect-setup

# 5. Start a worker in a separate terminal
make worker

# 6. Trigger a flow run manually
uv run prefect deployment run 'eod-prices-daily/daily'
# or with a specific date:
uv run prefect deployment run 'eod-prices-daily/backfill' -p trade_date=2026-05-09
```

---

## Local development (Docker)

**Prerequisites:** [OrbStack](https://orbstack.dev) or Docker Desktop, uv

```bash
# From the repo root

# 1. Create .env
cp apps/pipelines/.env.example apps/pipelines/.env
# Set EODHD_API_KEY — leave MOTHERDUCK_TOKEN blank for local DuckDB

# 2. Start the full stack (Prefect server + Postgres + worker)
make infra-up

# 3. Register work pool and deployments (once, waits for server to be ready)
make pipelines-setup

# UI: http://localhost:4200
```

Useful commands while running:

```bash
make infra-ps               # show service health
make infra-logs-pipelines   # tail worker logs
make infra-down             # stop everything
make infra-down-volumes     # stop + wipe Prefect DB (fresh start)
```

---

## Production (Coolify)

**Prerequisites:** A VPS with [Coolify](https://coolify.io) installed, this repo on GitHub.

### 1. Create a `docker-compose.prod.yml`

In `infra/docker-compose.prod.yml`, use the same services as `docker-compose.yml` with two changes:

```yaml
services:
  prefect-server:
    environment:
      PREFECT_SERVER_DATABASE_CONNECTION_URL: postgresql+asyncpg://prefect:${POSTGRES_PASSWORD}@postgres:5432/prefect
      PREFECT_UI_API_URL: https://prefect.yourdomain.com/api # ← public URL
    labels:
      - "coolify.port=4200"

  postgres:
    environment:
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD} # ← from Coolify secrets

  pipelines:
    environment:
      PREFECT_API_URL: http://prefect-server:4200/api # ← internal, not public
      PREFECT_WORK_DIR: /app
```

Key distinction: `PREFECT_UI_API_URL` is the **browser-facing** public URL. `PREFECT_API_URL` (worker) is the **internal** container-to-container address. Never set both to the same value.

### 2. Configure in Coolify

1. New resource → Docker Compose → point to `infra/docker-compose.prod.yml`
2. Add domain `prefect.yourdomain.com` → Coolify handles SSL automatically
3. Set environment secrets:
   - `POSTGRES_PASSWORD` — strong random password
   - `EODHD_API_KEY` — your EODHD key
   - `MOTHERDUCK_TOKEN` — your MotherDuck token (or leave blank for local DuckDB)
4. Deploy

### 3. Register deployments (once after first deploy)

```bash
export PREFECT_API_URL=https://prefect.yourdomain.com/api
cd apps/pipelines && make prefect-setup
```

---

## Day-to-day commands

```bash
# From apps/pipelines/
make check          # format + lint + typecheck + test (run before committing)
make test           # run all tests
make test-unit      # unit tests only
make lint-fix       # auto-fix ruff issues
make deploy         # re-register deployments after editing prefect.yaml
make deploy-dry     # preview what prefect.yaml would register (no server needed)
```

---

## Environment variables

| Variable           | Required | Description                                            |
| ------------------ | -------- | ------------------------------------------------------ |
| `EODHD_API_KEY`    | Yes      | EODHD API key — [eodhd.com](https://eodhd.com)         |
| `MOTHERDUCK_TOKEN` | No       | Leave blank → local `unique_stocks.db`                 |
| `PREFECT_API_URL`  | Yes      | Prefect server API URL                                 |
| `PREFECT_WORK_DIR` | Yes      | `.` (local) or `/app` (Docker)                         |
| `ENVIRONMENT`      | No       | `development` or `production` (default: `development`) |

---

## Initialise the database

```bash
# Local DuckDB
duckdb unique_stocks.db < ../../infra/scripts/init_motherduck.sql

# MotherDuck
MOTHERDUCK_TOKEN=<token> duckdb "md:unique_stocks" < ../../infra/scripts/init_motherduck.sql
```
