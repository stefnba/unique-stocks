# unique-stocks — Architecture & Build Guide

This document is the single source of truth for building this project. It covers the purpose, architecture decisions, tooling, repo structure, data design, and conventions. Read it fully before writing any code.

---

## 1. What We're Building

A self-hosted financial data platform with three responsibilities:

1. **Ingest** market data from external APIs on a schedule with prefect (EOD prices, securities lists, fundamentals, etc.) to our landing zone in S3
2. **Transform** raw data through a medallion lake (Bronze → Silver → Gold) using dbt
3. **Serve** the clean data through a web app with charts, search, and watchlists

The goal is lean, reliable, and cheap — not enterprise-scale. The entire stack should run on a single VPS on coolify with a budget of only a few $ per month.

---

## 2. Architecture Overview

```text
External APIs (EODHD)
        │
        ▼
┌───────────────────┐       ┌─────────────────────────────┐
│  apps/pipelines   │──①───▶│  S3 (landing zone)          │
│  (Python)         │       │  Raw API responses, exact   │
│  Prefect flows    │       │  bytes, immutable, JSON      │
│                   │◀──②───│  partitioned by date         │
└────────┬──────────┘       └─────────────────────────────┘
         │ ② parse S3 → write typed rows to bronze
         │ ③ trigger dbt after successful bronze write
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
- S3 serves two distinct roles:
  - **Landing zone** — raw API responses written as JSON by ingestion pipelines, before any parsing. This is the canonical replayable source.
  - **Parquet archive** — cold mirror of MotherDuck bronze/silver/gold schemas for backup and potential future query offload.
- Upgrade path exists: swap MotherDuck for S3 + Trino at 10TB+ scale

### 3.2 Medallion Architecture (Landing / Bronze / Silver / Gold)

S3 is the landing zone. MotherDuck holds three schemas.

| Layer   | Where               | Purpose                                                                 |
| ------- | ------------------- | ----------------------------------------------------------------------- |
| Landing | S3 `landing/`       | Raw API responses, exact bytes, immutable — written by Python pipelines |
| Bronze  | MotherDuck `bronze` | Typed, schema-enforced — parsed from S3 landing, append-only            |
| Silver  | MotherDuck `silver` | Deduplicated, normalized — no business logic (managed by dbt)           |
| Gold    | MotherDuck `gold`   | Analytics-ready: indicators, adjusted prices, aggregations (dbt)        |

Reprocessing bronze from scratch requires only S3 — no API re-fetch needed.

### 3.3 Prefect over Airflow

Prefect 3.x is Python-native and requires zero infrastructure for orchestration.
Airflow is too heavy for this scale. Prefect Cloud free tier is sufficient.

### 3.4 dbt Core for Transformations

All Bronze → Silver → Gold logic lives in dbt SQL models.
dbt runs are triggered by Prefect after each successful bronze write.

Two strict rules for Python ingestion code:

1. **Always write raw to S3 first.** The Prefect task that calls the API writes the exact JSON response to `s3://landing/`. No parsing yet.
2. **Parse S3 → bronze in a separate task.** A second task reads from S3, coerces types, enforces the schema, and writes typed rows to `bronze.*` in MotherDuck. No business logic — type safety only.

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

Option A — Streamlit (Python)

- Pro: no context switch from Python, fast to prototype, built-in charting
- Pro: directly queries MotherDuck with the same DuckDB connection used by pipelines
- Con: limited UI customisation, not suitable if studio becomes a public-facing product
- Best if: studio is an internal analytics tool / personal dashboard

Option B — Hono (TypeScript) backend + React + TanStack Router frontend

- Pro: full control over UI, production-grade web app, TypeScript end-to-end
- Pro: TanStack Router gives type-safe routing; React ecosystem for charts (TradingView Lightweight Charts)
- Con: separate runtime from pipelines, no shared Python code, more initial setup
- Best if: studio is a user-facing product with custom UX requirements

Until the decision is made:

- Do not build anything in `apps/studio/`
- Do not add studio-specific dependencies anywhere
- Design all gold layer dbt models to be queryable by either option without changes — the data layer is identical regardless of frontend choice

---

## 4. Structure

```text
unique-stocks/
├── apps/
│   └── pipelines/               # Python — Prefect ingestion app
│       ├── domains/
│       │   ├── eod_prices/      # EOD OHLCV (daily, trading days only)
│       │   │   ├── models.py
│       │   │   ├── flows.py
│       │   │   ├── tasks.py
│       │   │   └── parsers.py
│       │   ├── exchanges/       # List of stock exchanges (manual/monthly)
│       │   │   └── flows.py
│       │   ├── securities/      # Securities listed per exchange (weekly)
│       │   │   └── flows.py
│       │   └── fundamentals/    # Financials, dividends (quarterly)
│       │       └── flows.py
│       ├── core/                # Shared infrastructure only
│       │   ├── config.py        # Pydantic Settings — reads .env
│       │   ├── lake.py          # DuckDB / MotherDuck helpers
│       │   ├── models.py        # BronzeModel base class for domain models
│       │   ├── scheduler.py     # NYSE calendar helpers
│       │   ├── clients/
│       │   │   ├── http/
│       │   │   │   └── base.py      # HttpClientBase shared httpx client
│       │   │   ├── lake/
│       │   │   │   └── client.py    # DataLakeClient
│       │   │   └── storage/
│       │   │       └── s3/
│       │   │           └── base.py  # S3StorageClient
│       │   └── utils/
│       │       └── logging.py
│       ├── providers/           # Vendor-specific implementations
│       │   └── eodhd/           # EODHD HTTP data provider
│       │       ├── client.py
│       │       └── models.py
│       ├── tests/
│       │   ├── unit/
│       │   └── integration/
│       ├── prefect.yaml         # Deployment config — flows, schedules, work pool
│       ├── pyproject.toml       # Python deps for pipelines only
│       ├── Makefile             # Dev + deploy commands
│       └── Dockerfile
├── dbt_project/                 # dbt Core — transformation project
│   ├── models/
│   │   ├── staging/prices/
│   │   └── marts/prices/
│
├── infra/
│   ├── docker-compose.yml       # Local dev — Prefect server + Postgres + worker
│   └── scripts/
│       └── init_db.sql          # Create schemas and tables
│
├── Makefile                     # Monorepo-level commands (delegates to sub-makefiles)
└── README.md
```

### Why This Structure

- `apps/` follows the monorepo convention (Turborepo, Nx) — each subfolder is a deployable application
- `dbt_project/` sits at root because it is a standalone dbt project, not a Python package or web app
- `apps/pipelines/` has its own dependency file because it is independently deployable
- `studio/` tech stack is TBD — decided separately from pipelines, which are always Python
- `core/` contains only infrastructure (config, lake, clients, scheduler) — no domain models
- Domain models (`models.py`) live next to their `flows.py`/`tasks.py`/`parsers.py` — locality over centralisation

---

## 5. Data Domains & Schedules

Each domain has a different change frequency. This drives pipeline scheduling.

| Domain         | Data                               | Schedule                    | Partition Key                  |
| -------------- | ---------------------------------- | --------------------------- | ------------------------------ |
| `exchanges`    | List of stock exchanges            | Manual / monthly            | `snapshot_date`                |
| `securities`   | Securities listed per exchange     | Weekly (Monday 8am ET)      | `exchange`, `snapshot_date`    |
| `eod_prices`   | OHLCV end-of-day bars              | Daily (4:30pm ET, Mon–Fri)  | `year`, `month`                |
| `fundamentals` | Income stmt, balance sheet, ratios | Quarterly (earnings season) | `fiscal_year`, `fiscal_period` |

---

## 6. MotherDuck Schema Design

One database: `unique_stocks`. Three schemas.

### Bronze — Typed & Schema-Enforced (parsed from S3 landing)

```sql
-- Append-only. Populated by reading from S3 landing zone — never written directly
-- from API responses. s3_key provides lineage back to the exact raw file.
CREATE TABLE bronze.eod_prices (
    ingestion_id     UUID DEFAULT gen_random_uuid(),
    ticker           VARCHAR,        -- fully-qualified: 'AAPL.US'
    bar_date         DATE,
    open             DECIMAL,
    high             DECIMAL,
    low              DECIMAL,
    close            DECIMAL,
    volume           BIGINT,
    adjusted_close   DECIMAL,
    provider         VARCHAR,        -- 'eodhd'
    s3_key           VARCHAR,        -- source: 'landing/eodhd/eod_prices/date=2025-01-15/US.json'
    ingested_at      TIMESTAMPTZ DEFAULT now(),
    row_hash         VARCHAR         -- SHA-256 of (ticker, bar_date, provider) for dedup
);

CREATE TABLE bronze.securities (
    ingestion_id     UUID DEFAULT gen_random_uuid(),
    exchange         VARCHAR,
    code             VARCHAR,
    name             VARCHAR,
    snapshot_date    DATE,
    provider         VARCHAR,
    s3_key           VARCHAR,
    ingested_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE bronze.exchanges (
    ingestion_id     UUID DEFAULT gen_random_uuid(),
    code             VARCHAR,
    name             VARCHAR,
    snapshot_date    DATE,
    provider         VARCHAR,
    s3_key           VARCHAR,
    ingested_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE bronze.fundamentals (
    ingestion_id     UUID DEFAULT gen_random_uuid(),
    ticker           VARCHAR,
    fiscal_year      INTEGER,
    fiscal_period    VARCHAR,        -- 'Q1', 'Q2', 'Q3', 'Q4', 'TTM'
    report_type      VARCHAR,        -- 'income_statement', 'balance_sheet', 'cash_flow'
    provider         VARCHAR,
    s3_key           VARCHAR,
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

### S3 Layout

```text
s3://my-stock-stack/
│
├── landing/                                    ← Raw API responses, exact bytes, immutable
│   └── eodhd/
│       ├── eod_prices/date=2025-01-15/
│       │   ├── US.json                         -- bulk response for one exchange+date
│       │   └── LSE.json
│       ├── securities/date=2025-01-01/
│       │   └── NASDAQ.json
│       ├── exchanges/date=2025-01-01/
│       │   └── all.json
│       └── fundamentals/ticker=AAPL/date=2025-01-01/
│           └── AAPL.json
│
├── bronze/                                     ← Parquet mirror of MotherDuck bronze
│   ├── exchanges/snapshot_date=2025-01-01/
│   ├── securities/exchange=NASDAQ/snapshot_date=2025-01-01/
│   ├── eod_prices/year=2025/month=01/
│   └── fundamentals/fiscal_year=2024/fiscal_period=Q4/
│
├── silver/
│   └── ... (mirrors bronze partition structure)
│
└── gold/
    ├── prices_daily/year=2025/month=01/
    ├── securities_current/
    └── fundamentals_ttm/
```

---

## 7. Prefect Flow Design

### Flow vs Task Separation

**Tasks** (`tasks.py`) — atomic, retryable, independently testable units of work.
**Flows** (`flows.py`) — thin orchestrators that wire tasks together. No business logic.
**Parsers** (`parsers.py`) — pure parsing/normalisation functions, no Prefect decorators, fully unit-testable.

Domain flows keep provider fetches, parsing, and bronze writes in separate units:

```python
# tasks.py — three tasks, strict separation of concerns

@task(retries=3, retry_delay_seconds=exponential_backoff(10))
async def fetch_eod_prices_bulk(exchange: str, bar_date: date) -> list[EODBulkPriceRaw]:
    """Fetch and schema-validate raw EODHD rows for an exchange/date."""
    async with EODHDClient(api_key=get_settings().eodhd_api_key) as client:
        return await client.get_eod_prices_bulk(exchange=exchange, bar_date=bar_date)

@task
def parse_eod_prices(raw_rows: list[EODBulkPriceRaw], bar_date: date, exchange: str) -> list[EODBar]:
    """Convert raw provider rows into validated EODBar domain models."""
    valid, rejected = parse_eod_bars(raw_rows, expected_date=bar_date, exchange=exchange)
    return valid

@task
def write_bronze_eod_prices(bars: list[EODBar], exchange: str, bar_date: date) -> int:
    """Write validated bars to bronze.eod_prices."""
    records = bars_to_bronze_records(bars)
    return lake.insert_rows("bronze", "eod_prices", records)

# flows.py — wires tasks, handles schedule/idempotency logic
@flow(name="eod-prices-daily")
async def eod_prices_flow(trade_date: date | None = None):
    trade_date = trade_date or last_completed_trading_day()
    if not is_trading_day(trade_date):
        return
    for exchange in V1_EXCHANGES:
        raw_rows = await fetch_eod_prices_bulk(exchange=exchange, bar_date=trade_date)
        bars = parse_eod_prices(raw_rows, bar_date=trade_date, exchange=exchange)
        write_bronze_eod_prices(bars, exchange=exchange, bar_date=trade_date)
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

# Backfill — manual deployment of eod_prices_flow with explicit trade_date
```

### Idempotency Rule

Before writing data, flows check what already exists in bronze:

```python
if lake.already_ingested_exchange_date("eod_prices", exchange, bar_date):
    return 0
```

Re-running any flow must produce the same result. No duplicates. No errors on re-run.

---

## 8. Core Module Contracts

### `core/config.py`

```python
from functools import lru_cache
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

type Environment = Literal["development", "production"]

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    eodhd_api_key: str
    motherduck_token: str = ""
    s3_bucket: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_region: str = "ap-southeast-2"
    prefect_api_url: str = "http://127.0.0.1:4200/api"
    prefect_api_key: str = ""
    environment: Environment = "development"

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str: ...

    @property
    def is_production(self) -> bool: ...

    @property
    def is_development(self) -> bool: ...

    @property
    def duckdb_connection_string(self) -> str:
        if self.motherduck_token:
            return f"md:unique_stocks?motherduck_token={self.motherduck_token}"
        return "unique_stocks.db"

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
```

`Settings()` is never instantiated at import time. `get_settings()` is called only when a task actually executes, so `prefect deploy` and test imports work without secrets present.

### `core/clients/http/base.py`

```python
class HttpClientBase(ABC):
    PROVIDER: ClassVar[str]
    BASE_URL: ClassVar[str]

    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *_: object) -> None: ...
    async def _request(self, path: str, *, method: HttpMethod = "GET", params: dict[str, Any] | None = None, json: dict[str, Any] | None = None) -> Any: ...
    async def _get(self, path: str, *, model: type[T], params: dict[str, Any] | None = None) -> T: ...
    async def _get_list(self, path: str, *, model: type[T], params: dict[str, Any] | None = None) -> list[T]: ...
```

Provider clients such as `providers/eodhd/client.py` inherit this shared httpx session, auth, logging, timeout, and Pydantic validation behavior. Provider-specific API methods live in the provider client, not in the base class.

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

def execute(sql: str, params: list[Any] | None = None) -> None: ...
def query(sql: str, params: list[Any] | None = None) -> list[dict]: ...
def query_one(sql: str, params: list[Any] | None = None) -> dict | None: ...
def table_exists(schema: str, table: str) -> bool: ...
def insert_rows(schema: str, table: str, rows: list[dict]) -> int: ...
def already_ingested_dates(table: str, ticker: str, start: date, end: date) -> set[date]: ...
def already_ingested_exchange_date(table: str, exchange: str, bar_date: date) -> bool: ...
def record_run_start(flow_name: str) -> str: ...
def record_run_complete(run_id: str, rows_written: int) -> None: ...
def record_run_failed(run_id: str, error: str) -> None: ...
```

### `core/clients/lake/client.py`

```python
class DataLakeClient:
    """Client to save and retrieve data from the data lake (MotherDuck)."""
```

### `core/clients/storage/s3/base.py`

```python
class S3StorageClient:
    """Client to save and retrieve data from S3."""
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
