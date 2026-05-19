# Pipelines

`apps/pipelines` is the Prefect 3 ingestion app for Unique Stocks. It fetches market data from providers, validates provider responses, writes typed Bronze records through the lake client, and coordinates scheduled runs.

The active path is EODHD end-of-day prices. Exchanges, securities, and fundamentals are planned domain flows.

## Status

| Domain | Status | Schedule |
| --- | --- | --- |
| `eod_prices` | Active | Weekdays after market close, plus manual backfill. |
| `exchanges` | Planned stub | Manual or monthly. |
| `securities` | Planned stub | Weekly. |
| `fundamentals` | Planned stub | Manual or quarterly. |

## Project structure

```text
apps/pipelines/
├── domains/       Domain models, parsers, tasks, and flows
├── core/          Shared config, scheduler, logging, lake, and storage clients
├── providers/     Provider-specific clients and raw response models
├── tests/         Unit and integration tests
├── prefect.yaml   Prefect deployment definitions
├── pyproject.toml Python dependencies
├── Makefile       App-level commands
└── Dockerfile     Worker image
```

Domain code is organized under `domains/<domain>/`. Add `models.py`, `parsers.py`, `tasks.py`, and `flows.py` as the domain needs them.

## Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/)
- Docker Desktop or OrbStack for Docker-based local infrastructure
- An EODHD API key for live provider runs

## Environment

Create local configuration from the example file:

```bash
cd apps/pipelines
cp .env.example .env
```

Set at least `EODHD_API_KEY` for live provider runs. Leave `MOTHERDUCK_TOKEN` blank to use local DuckDB.

| Variable | Required | Description |
| --- | --- | --- |
| `EODHD_API_KEY` | Yes for live runs | EODHD API key. |
| `MOTHERDUCK_TOKEN` | No | Blank uses local `unique_stocks.db`; set for MotherDuck. |
| `S3_BUCKET` | No | S3 bucket for landing or archival storage. |
| `AWS_ACCESS_KEY_ID` | No | AWS access key when S3 is enabled. |
| `AWS_SECRET_ACCESS_KEY` | No | AWS secret key when S3 is enabled. |
| `AWS_REGION` | No | AWS region, default `ap-southeast-2`. |
| `PREFECT_API_URL` | Yes | Prefect API URL for workers and deploy commands. |
| `PREFECT_WORK_DIR` | Yes | `.` locally, `/app` in Docker. |
| `ENVIRONMENT` | No | `development` or `production`. |

## Local development without Docker

Use this path for fast Python feedback when you do not need the Docker Compose stack.

```bash
cd apps/pipelines
uv sync
make prefect-server
```

In another terminal:

```bash
cd apps/pipelines
make prefect-setup
make worker
```

Trigger a deployment manually:

```bash
uv run prefect deployment run 'eod-prices-daily/daily'
uv run prefect deployment run 'eod-prices-daily/backfill' -p trade_date=2026-05-09
```

## Local development with Docker

Use this path when you want the local stack to mirror production more closely.

From the repository root:

```bash
cp apps/pipelines/.env.example apps/pipelines/.env
make infra-up
make pipelines-setup
```

Useful root commands:

```bash
make infra-ps
make infra-logs-pipelines
make infra-down
make infra-down-volumes
```

Prefect UI runs at <http://localhost:4200>.

## Database initialization

Initialize local DuckDB:

```bash
cd apps/pipelines
duckdb unique_stocks.db < ../../infra/scripts/init_db.sql
```

Initialize MotherDuck:

```bash
cd apps/pipelines
MOTHERDUCK_TOKEN=<token> duckdb "md:unique_stocks" < ../../infra/scripts/init_db.sql
```

## Quality checks

Run the full app quality gate before committing pipeline changes:

```bash
cd apps/pipelines
make check
```

Focused commands:

```bash
make test
make test-unit
make test-integration
make lint
make lint-fix
make format-check
make typecheck
```

## Prefect deployments

`prefect.yaml` defines the app deployments and schedules.

```bash
cd apps/pipelines
make deploy-dry
make deploy
```

`make prefect-setup` creates the default work pool and registers all deployments. Run it once per new Prefect environment, then use `make deploy` after changing deployment definitions.

## Production notes

Production is expected to run through Docker Compose on a small VPS, with Coolify handling builds, deployment, domains, and SSL.

Keep these addresses distinct:

- `PREFECT_UI_API_URL`: browser-facing public API URL used by the Prefect UI.
- `PREFECT_API_URL`: worker-facing API URL used by the pipelines container.

Set production secrets in the deployment platform, not in git:

- `POSTGRES_PASSWORD`
- `EODHD_API_KEY`
- `MOTHERDUCK_TOKEN`
- S3 credentials when S3 landing or archival storage is enabled

After the first production deploy, register deployments against the production Prefect API:

```bash
export PREFECT_API_URL=https://prefect.yourdomain.com/api
cd apps/pipelines
make prefect-setup
```
