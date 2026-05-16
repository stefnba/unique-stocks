# my-stock-stack — Architecture & Build Guide

> This document is the single source of truth for building this project.
> It covers the purpose, architecture decisions, tooling, repo structure,
> data design, and conventions. Read it fully before writing any code.

---

## 1. What We're Building

A self-hosted financial data platform with three responsibilities:

1. **Ingest** market data from external APIs on a schedule (EOD prices, securities lists, fundamentals, etc.)
2. **Transform** raw data through a medallion lake (Bronze → Silver → Gold) using dbt
3. **Serve** the clean data through a web app with charts, search, and watchlists

The goal is lean, reliable, and cheap — not enterprise-scale. The entire stack should run on a single VPS for under $50/month at 5,000-ticker scale.

---

## 2. Architecture Overview

```text
External APIs (EODHD)
        │
        ▼
┌───────────────────┐
│  apps/pipelines   │  Prefect flows — scheduled ingestion
│  (Python)         │  Writes raw data to Bronze layer
└────────┬──────────┘
         │ triggers
         ▼
┌───────────────────┐
│   dbt_project/    │  dbt Core — SQL transformations
│                   │  Bronze → Silver → Gold
└────────┬──────────┘
         │ writes clean data to
         ▼
┌───────────────────┐
│  MotherDuck       │  Primary data store (DuckDB cloud)
│  (3 schemas)      │  bronze / silver / gold
│                   │  Parquet files on S3 as backup/archive
└────────┬──────────┘
         │ queried by
         ▼
┌───────────────────┐
│  apps/studio      │  Backend + Frontend — stack TBD (see section 3.7)
│                   │  Charts, search, watchlists
└───────────────────┘
```

---

## 3. Key Architecture Decisions

These decisions are final for v1. Do not re-litigate them.

### 3.1 MotherDuck over S3 + Trino

MotherDuck (managed DuckDB cloud) is the query engine and primary store.

- Zero ops — no cluster to manage
- DuckDB SQL is fast enough for our scale (<100GB)
- Parquet files on S3 are used for cold archival only, not active querying
- Upgrade path exists: swap MotherDuck for S3 + Trino at 10TB+ scale

### 3.2 Medallion Architecture (Bronze / Silver / Gold)

Three schemas in one MotherDuck database, mirrored as Parquet on S3.

| Layer  | Schema   | Purpose                                                    |
| ------ | -------- | ---------------------------------------------------------- |
| Bronze | `bronze` | Raw API responses, immutable, append-only                  |
| Silver | `silver` | Typed, deduplicated, normalized — no business logic        |
| Gold   | `gold`   | Analytics-ready: indicators, adjusted prices, aggregations |

### 3.3 Prefect over Airflow

Prefect 3.x is Python-native and requires zero infrastructure for orchestration.
Airflow is too heavy for this scale. Prefect Cloud free tier is sufficient.

### 3.4 dbt Core for Transformations

All Bronze → Silver → Gold logic lives in dbt SQL models.
dbt runs are triggered by Prefect after each successful ingestion.
Never transform data inside Python ingestion code — land raw first, always.

### 3.5 Plain Parquet over Apache Iceberg

No Iceberg for v1. Our data is append-only daily bars.
Iceberg adds complexity (catalog, table format) without benefit at this scale.
Revisit when: upserts are needed at scale, or a second compute engine is added.

### 3.6 EODHD as Primary Data Provider

EODHD (eodhd.com) provides a single API for:

- EOD prices, exchanges, securities lists, fundamentals, dividends, splits
  All in one API key. Simpler than managing Polygon + Alpha Vantage + yfinance.
  The provider abstraction allows adding more sources later without restructuring.

### 3.7 Studio Stack — Decision Pending ⚠️

The `apps/studio` tech stack has not been decided yet. Two options are on the table:

**Option A — Streamlit (Python)**

- Pro: no context switch from Python, fast to prototype, built-in charting
- Pro: directly queries MotherDuck with the same DuckDB connection used by pipelines
- Con: limited UI customisation, not suitable if studio becomes a public-facing product
- Best if: studio is an internal analytics tool / personal dashboard

**Option B — Hono (TypeScript) backend + React + TanStack Router frontend**

- Pro: full control over UI, production-grade web app, TypeScript end-to-end
- Pro: TanStack Router gives type-safe routing; React ecosystem for charts (TradingView Lightweight Charts)
- Con: separate runtime from pipelines, no shared Python code, more initial setup
- Best if: studio is a user-facing product with custom UX requirements

**Until the decision is made:**

- Do not build anything in `apps/studio/`
- Do not add studio-specific dependencies anywhere
- Design all gold layer dbt models to be queryable by either option without changes — the data layer is identical regardless of frontend choice

---

## 4. Monorepo Structure

```text
my-stock-stack/
├── apps/
│   ├── pipelines/               # Python — Prefect ingestion app
│   │   ├── domains/
│   │   │   ├── exchanges/       # List of stock exchanges (manual/monthly)
│   │   │   │   ├── models.py    # Pydantic v2 models for this domain
│   │   │   │   ├── flows.py
│   │   │   │   ├── tasks.py
│   │   │   │   └── parsers.py
│   │   │   ├── securities/      # Securities listed per exchange (weekly)
│   │   │   │   ├── models.py
│   │   │   │   ├── flows.py
│   │   │   │   ├── tasks.py
│   │   │   │   └── parsers.py
│   │   │   ├── eod_prices/      # EOD OHLCV (daily, trading days only)
│   │   │   │   ├── models.py
│   │   │   │   ├── flows.py
│   │   │   │   ├── tasks.py
│   │   │   │   └── parsers.py
│   │   │   └── fundamentals/    # Financials, dividends (quarterly)
│   │   │       ├── models.py
│   │   │       ├── flows.py
│   │   │       ├── tasks.py
│   │   │       └── parsers.py
│   │   ├── core/                # Shared infrastructure only
│   │   │   ├── config.py        # Pydantic Settings — reads .env
│   │   │   ├── lake.py          # MotherDuck read/write helpers
│   │   │   ├── models.py        # BronzeModel base class for all domain models
│   │   │   ├── scheduler.py     # NYSE calendar, is_trading_day()
│   │   │   ├── clients/
│   │   │   │   ├── base.py      # Abstract BaseClient (ABC)
│   │   │   │   └── eodhd.py     # EODHD API wrapper (httpx + retry)
│   │   │   └── utils/
│   │   │       ├── logging.py
│   │   │       └── rate_limiter.py
│   │   ├── tests/
│   │   │   ├── unit/
│   │   │   └── integration/
│   │   ├── prefect.yaml         # Deployment config — all flows, schedules, work pool
│   │   ├── pyproject.toml       # Python deps for pipelines only
│   │   ├── Makefile             # Dev + deploy commands
│   │   └── Dockerfile
│   │
│   └── studio/                  # Web app — stack TBD (Python or TypeScript)
│       ├── backend/             # TBD: Streamlit (Python) or Hono (TypeScript)
│       │   ├── main.py
│       │   ├── routers/
│       │   └── schemas/
│       └── frontend/            # TBD: React + TanStack Router (if Hono chosen)
│           ├── app/
│           └── components/
│
├── dbt_project/                 # dbt Core — all transformation logic
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/             # Bronze → Silver (one subfolder per domain)
│   │   │   ├── eod_prices/
│   │   │   ├── exchanges/
│   │   │   ├── securities/
│   │   │   └── fundamentals/
│   │   └── marts/               # Silver → Gold
│   │       ├── eod_prices/
│   │       ├── search/
│   │       └── fundamentals/
│   ├── tests/
│   └── macros/
│
├── infra/
│   ├── docker-compose.yml       # Local dev — Prefect server + Postgres + worker
│   └── scripts/
│       ├── init_motherduck.sql  # Create schemas and tables
│       └── setup_s3.sh
│
├── Makefile                     # Monorepo-level commands (delegates to sub-makefiles)
└── README.md
```

### Why This Structure

- `apps/` follows the monorepo convention (Turborepo, Nx) — each subfolder is a deployable application
- `dbt_project/` sits at root because it is a standalone dbt project, not a Python package or web app
- `apps/pipelines/` and `apps/studio/` have their own dependency files — they are independent deploys
- `studio/` tech stack is TBD — decided separately from pipelines, which are always Python
- `core/` contains only infrastructure (config, lake, clients, scheduler) — no domain models
- Domain models (`models.py`) live next to their `flows.py`/`tasks.py`/`parsers.py` — locality over centralisation

---

## 5. Data Domains & Schedules

Each domain has a different change frequency. This drives pipeline scheduling.

| Domain          | Data                               | Schedule                    | Partition Key                  |
| --------------- | ---------------------------------- | --------------------------- | ------------------------------ |
| `exchanges`     | List of stock exchanges            | Manual / monthly            | `snapshot_date`                |
| `securities`    | Securities listed per exchange     | Weekly (Monday 8am ET)      | `exchange`, `snapshot_date`    |
| `eod_prices`    | OHLCV end-of-day bars              | Daily (4:30pm ET, Mon–Fri)  | `year`, `month`                |
| `fundamentals`  | Income stmt, balance sheet, ratios | Quarterly (earnings season) | `fiscal_year`, `fiscal_period` |

---

## 6. MotherDuck Schema Design

One database: `unique_stocks`. Three schemas.

### Bronze — Raw Ingestion

```sql
-- Never modified after write. Source of truth for replay.
CREATE TABLE bronze.eod_prices (
    ingestion_id     UUID DEFAULT gen_random_uuid(),
    ticker           VARCHAR,
    bar_date         DATE,
    provider         VARCHAR,        -- 'eodhd'
    raw_json         JSON,           -- verbatim API response
    ingested_at      TIMESTAMPTZ DEFAULT now(),
    row_hash         VARCHAR         -- SHA-256 for dedup
);

CREATE TABLE bronze.securities (
    ingestion_id     UUID DEFAULT gen_random_uuid(),
    exchange         VARCHAR,
    snapshot_date    DATE,
    provider         VARCHAR,
    raw_json         JSON,
    ingested_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE bronze.exchanges (
    ingestion_id     UUID DEFAULT gen_random_uuid(),
    snapshot_date    DATE,
    provider         VARCHAR,
    raw_json         JSON,
    ingested_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE bronze.fundamentals (
    ingestion_id     UUID DEFAULT gen_random_uuid(),
    ticker           VARCHAR,
    fiscal_year      INTEGER,
    fiscal_period    VARCHAR,        -- 'Q1', 'Q2', 'Q3', 'Q4', 'TTM'
    report_type      VARCHAR,        -- 'income_statement', 'balance_sheet', 'cash_flow'
    provider         VARCHAR,
    raw_json         JSON,
    ingested_at      TIMESTAMPTZ DEFAULT now()
);
```

### Silver — Typed & Cleaned (managed by dbt)

```sql
-- dbt staging models write here
-- Type-cast, deduplicated, normalized. No business logic.
silver.eod_prices       -- typed OHLCV columns, UTC timestamps
silver.securities       -- typed security reference data
silver.exchanges        -- typed exchange reference data
silver.fundamentals     -- typed financial statement rows
silver.dividends        -- split out from fundamentals raw
silver.splits           -- split out from fundamentals raw
```

### Gold — Analytics Ready (managed by dbt)

```sql
-- dbt mart models write here
-- Business logic: adjusted prices, indicators, aggregations
gold.prices_daily       -- adj_close, SMA, RSI, MACD, pct_change
gold.securities_current -- latest snapshot only (no history)
gold.search_index       -- ticker, name, exchange, sector (for search)
gold.fundamentals_ttm   -- trailing twelve months, computed ratios
```

### Pipeline Metadata (separate schema)

```sql
-- Prefect writes run state here for idempotency
CREATE TABLE pipeline.runs (
    run_id           UUID DEFAULT gen_random_uuid(),
    flow_name        VARCHAR,
    status           VARCHAR,        -- 'running', 'completed', 'failed', 'skipped'
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    rows_written     INTEGER,
    error_message    TEXT
);
```

### S3 Parquet Layout (mirrors MotherDuck schemas)

```text
s3://my-stock-stack/
├── bronze/
│   ├── exchanges/snapshot_date=2025-01-01/
│   ├── securities/exchange=NASDAQ/snapshot_date=2025-01-01/
│   ├── prices/eod/year=2025/month=01/
│   └── fundamentals/fiscal_year=2024/fiscal_period=Q4/
├── silver/
│   └── ... (mirrors bronze partition structure)
└── gold/
    ├── prices_daily/year=2025/month=01/
    ├── securities_current/             # No date partition — always latest
    └── fundamentals_ttm/
```

---

## 7. Prefect Flow Design

### Flow vs Task Separation

**Tasks** (`tasks.py`) — atomic, retryable, independently testable units of work.
**Flows** (`flows.py`) — thin orchestrators that wire tasks together. No business logic.
**Parsers** (`parsers.py`) — pure parsing/normalisation functions, no Prefect decorators, fully unit-testable.

```python
# tasks.py — has @task decorator, handles retry/logging
@task(retries=3, retry_delay_seconds=exponential_backoff(10))
async def fetch_eod_prices(ticker: str, bar_date: date) -> list[EODBar]: ...

@task
async def write_bronze(bars: list[EODBar], bar_date: date) -> None: ...

# flows.py — has @flow decorator, wires tasks, handles schedule logic
@flow(name="eod-prices-daily")
async def eod_prices_flow(trade_date: date | None = None):
    trade_date = trade_date or date.today()
    if not is_trading_day(trade_date):
        return  # clean skip, not a failure
    tickers = await get_ticker_universe()
    bars = await fetch_eod_prices.map(tickers, unmapped(trade_date))
    await write_bronze(bars, trade_date)
```

### Flow Schedule Reference

```python
# apps/pipelines/domains/eod_prices/flows.py
@flow
async def eod_prices_flow(): ...
# Schedule: CronSchedule("30 16 * * 1-5")  — 4:30pm ET Mon–Fri

# apps/pipelines/domains/securities/flows.py
@flow
async def securities_flow(): ...
# Schedule: CronSchedule("0 8 * * 1")  — Monday 8am

# apps/pipelines/domains/exchanges/flows.py
@flow
async def exchanges_flow(): ...
# Schedule: Manual only

# apps/pipelines/domains/fundamentals/flows.py
@flow
async def fundamentals_flow(): ...
# Schedule: Manual trigger, or quarterly CronSchedule

# Backfill — parameterized, manual trigger always
@flow
async def backfill_flow(domain: str, start_date: date, end_date: date): ...
```

### Idempotency Rule

Before fetching any data, every flow checks what already exists in bronze:

```python
# Check bronze table before fetching
already_ingested = lake.query("""
    SELECT DISTINCT bar_date FROM bronze.eod_prices
    WHERE ticker = ? AND bar_date BETWEEN ? AND ?
""", ticker, start, end)

missing = [d for d in trading_days(start, end) if d not in already_ingested]
# Only fetch missing dates
```

Re-running any flow must produce the same result. No duplicates. No errors on re-run.

---

## 8. Core Module Contracts

### `core/config.py`

```python
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    eodhd_api_key: str
    motherduck_token: str = ""
    environment: str = "development"

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
```

`Settings()` is never instantiated at import time. `get_settings()` is called only when a task actually executes, so `prefect deploy` and test imports work without secrets present.

### `core/clients/base.py`

```python
from abc import ABC, abstractmethod

class BaseClient(ABC):
    @abstractmethod
    async def get_eod_prices(self, ticker: str, date: date) -> list[dict]: ...

    @abstractmethod
    async def get_securities(self, exchange: str) -> list[dict]: ...

    @abstractmethod
    async def get_exchanges(self) -> list[dict]: ...

    @abstractmethod
    async def get_fundamentals(self, ticker: str) -> dict: ...
```

### `core/models.py`

All domain models inherit from `BronzeModel`. `to_bronze_record()` is implemented once here — never repeated in domain models.

```python
class BronzeModel(BaseModel):
    def to_bronze_record(self, provider: str = "eodhd") -> dict[str, Any]:
        payload = self.model_dump(mode="json")   # Decimal→str, date→ISO handled by Pydantic
        raw_json = json.dumps(payload, sort_keys=True)
        row_hash = hashlib.sha256(raw_json.encode()).hexdigest()
        return {**payload, "provider": provider, "raw_json": raw_json, "row_hash": row_hash}
```

### `domains/<domain>/models.py`

Pydantic models live alongside the domain that owns them. Each inherits `BronzeModel`.

```python
# domains/eod_prices/models.py
from core.models import BronzeModel

class EODBar(BronzeModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    ticker: str
    bar_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    adjusted_close: Decimal | None = None

    @model_validator(mode="after")
    def validate_ohlc(self) -> "EODBar": ...
```

### `core/lake.py`

```python
# Wraps MotherDuck connection. All reads/writes go through here.
# Never import duckdb directly in pipeline code — use this module.

async def write_bronze(table: str, records: list[dict], partition: dict) -> None: ...
async def query(sql: str, *params) -> list[dict]: ...
async def table_exists(schema: str, table: str) -> bool: ...
```

---

## 9. dbt Project Conventions

### Model Naming

```text
staging/prices/stg_prices__eod.sql          # staging: stg_{domain}__{entity}
marts/prices/prices_daily.sql               # mart: no prefix, descriptive name
marts/search/search_index.sql
```

### Materialization Strategy

| Layer                                     | Materialization | Reason                          |
| ----------------------------------------- | --------------- | ------------------------------- |
| Staging (silver)                          | `incremental`   | Process only new bronze records |
| Marts (gold)                              | `incremental`   | Process only new silver records |
| Reference (exchanges, securities_current) | `table`         | Small, always full refresh      |

### Incremental Pattern

All incremental models use a 200-day lookback to correctly recompute rolling indicators:

```sql
{{ config(materialized='incremental', unique_key=['ticker', 'bar_date']) }}

SELECT * FROM {{ ref('stg_prices__eod') }}
{% if is_incremental() %}
WHERE bar_date >= (
    SELECT MAX(bar_date) - INTERVAL '200 days' FROM {{ this }}
)
{% endif %}
```

### Required dbt Tests (minimum per model)

- `not_null` on all primary key columns
- `unique` on all primary key combinations
- `accepted_values` on enum columns (provider, exchange, period)
- Custom: `ohlc_sanity` — high >= open, high >= close, low <= open, low <= close
- Custom: `row_count_vs_expected` — flag if <95% of expected tickers have data for a date

---

## 10. Tech Stack Reference

| Layer                 | Technology                                      | Version |
| --------------------- | ----------------------------------------------- | ------- |
| Python runtime        | Python                                          | 3.14+   |
| Dependency management | uv                                              | latest  |
| Orchestration         | Prefect                                         | 3.7+    |
| HTTP client           | httpx                                           | latest  |
| Data validation       | Pydantic                                        | v2      |
| Data lake query       | DuckDB / MotherDuck                             | latest  |
| Transformation        | dbt Core + dbt-duckdb                           | 1.8+    |
| Studio backend        | Streamlit **or** Hono — TBD                     | —       |
| Studio frontend       | React + TanStack Router (if Hono) — TBD         | —       |
| Charts                | TradingView Lightweight Charts (if React) — TBD | v5      |
| Containerisation      | Docker + Docker Compose                         | latest  |
| Monitoring            | Prometheus + Grafana                            | latest  |

---

## 11. Environment Variables

All secrets live in `.env` at the app root (never committed). Copy `.env.example` to get started.

```bash
# .env.example — apps/pipelines/.env

# Data provider
EODHD_API_KEY=your_key_here

# MotherDuck — leave blank to use local DuckDB (unique_stocks.db) in dev
MOTHERDUCK_TOKEN=

# S3 (optional for v1 — cold archival only)
S3_BUCKET=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=ap-southeast-2

# Prefect — self-hosted server (see infra/docker-compose.yml)
# Local dev without Docker:
PREFECT_API_URL=http://localhost:4200/api
# Inside docker-compose: PREFECT_API_URL=http://prefect-server:4200/api
# Coolify production:     PREFECT_API_URL=https://prefect.yourdomain.com/api

# Working directory for prefect.yaml pull step (local dev: . / Docker: /app)
PREFECT_WORK_DIR=.

# App
ENVIRONMENT=development
```

---

## 12. Local Development Setup

### Option A — Docker (recommended, mirrors production)

```bash
git clone https://github.com/you/unique-stocks
cd unique-stocks

cp apps/pipelines/.env.example apps/pipelines/.env
# fill in EODHD_API_KEY — leave MOTHERDUCK_TOKEN blank for local DuckDB

make infra-up          # start Prefect server + Postgres + worker
make pipelines-setup   # create work pool + register all deployments (once)
# Prefect UI: http://localhost:4200

make infra-logs-pipelines   # tail worker output
make infra-down             # stop everything
```

### Option B — No Docker (lighter, faster feedback)

```bash
cd apps/pipelines
cp .env.example .env   # fill in EODHD_API_KEY

uv sync                         # install deps
make prefect-server             # terminal 1 — starts server at http://localhost:4200
make prefect-setup              # terminal 2 — register work pool + deployments
make worker                     # terminal 2 — start worker

# Trigger a run from the UI or CLI:
uv run prefect deployment run 'eod-prices-daily/daily'
uv run prefect deployment run 'eod-prices-daily/backfill' -p trade_date=2026-05-09
```

### dbt

```bash
cd dbt_project
dbt deps
dbt run --select staging
dbt test
```

---

## 13. Coding Conventions

### Python

- Type hints on every function signature — no exceptions
- Pydantic v2 models for all external data, `extra="forbid"` to catch API drift early
- `async/await` throughout — httpx async client, async MotherDuck queries
- Never transform data inside a `@task` that also does I/O — separate fetch/transform/write into distinct tasks
- Log with structlog, not print() — every log line gets `ticker` and `bar_date` as structured fields
- One `@flow` per domain per schedule — no mega-flows

### dbt

- Never put business logic in staging models — staging is type-casting and renaming only
- Every model gets a `.yml` description file with column descriptions
- Test files mirror model structure exactly
- `ref()` over hardcoded table names always

### General

- No secrets in code or git — `.env` only
- Every new domain under `domains/` follows the same pattern: `models.py` + `flows.py` + `tasks.py` + `parsers.py`
- README in every app folder explaining how to run it standalone

---

## 14. What v1 Excludes (Intentionally)

These are out of scope for the initial build. Do not add them.

- Intraday / real-time data (daily EOD only)
- Options chain data
- Crypto or FX data
- Multi-tenant / SaaS mode
- Apache Iceberg table format
- Trino query engine
- User authentication in studio (read-only, public for now)
- Alerting / notifications

---

## 15. Git Conventions

- `main` — production-ready code only
- `archive/v1` — old codebase, preserved for reference, do not merge back
- Feature branches: `feat/eod-pipeline`, `feat/dbt-prices-mart`
- Commit style: `feat:`, `fix:`, `chore:`, `docs:` prefixes

---

_Built to be lean. Add complexity only when you have a concrete reason._
