# pipelines

Prefect ingestion flows for the unique-stocks data platform.

## Setup

```bash
cd apps/pipelines

# Install deps with uv
uv sync

# Copy env file and fill in your keys
cp .env.example .env
```

Required vars in `.env`:

| Variable           | Description                                       |
| ------------------ | ------------------------------------------------- |
| `EODHD_API_KEY`    | Your EODHD API key                                |
| `MOTHERDUCK_TOKEN` | Leave blank for local DuckDB (`unique_stocks.db`) |

## Initialise the database

```bash
# Local DuckDB (dev)
duckdb unique_stocks.db < ../../infra/scripts/init_db.sql

# MotherDuck (prod) — from the repo root
MOTHERDUCK_TOKEN=<token> duckdb "md:unique_stocks?motherduck_token=<token>" < infra/scripts/init_db.sql
```

## Run the EOD prices flow manually

```bash
# Uses today / last trading day by default
uv run python -m pipelines.prices.flows

# Specific date
uv run python -c "
import asyncio
from datetime import date
from pipelines.prices.flows import eod_prices_flow
asyncio.run(eod_prices_flow(trade_date=date(2026, 5, 9)))
"
```

## Project structure

```text
pipelines/
  prices/       flows.py  tasks.py  transforms.py
  metadata/     (next)
  fundamentals/ (next)
shared/
  config.py     Pydantic Settings
  lake.py       DuckDB/MotherDuck helpers
  scheduler.py  NYSE calendar
  clients/      base.py  eodhd.py
  schemas/      prices.py
tests/
  unit/
  integration/
```

## Run tests

```bash
uv run pytest
```
