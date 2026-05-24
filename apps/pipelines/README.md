# Pipelines

`apps/pipelines` is the Prefect 3 ingestion app for Unique Stocks. It fetches market data from providers, validates provider responses, writes typed Bronze records through the lake client, and coordinates scheduled runs.

The active path is EODHD end-of-day prices. Exchanges and instruments are reference flows; fundamentals is planned.

## Status

| Domain         | Status         | Schedule                                           |
| -------------- | -------------- | -------------------------------------------------- |
| `eod_prices`   | Active         | Weekdays after market close, plus manual backfill. |
| `exchanges`    | Reference flow | Manual or monthly.                                 |
| `instruments`  | Reference flow | Weekly.                                            |
| `fundamentals` | Planned stub   | Manual or quarterly.                               |

## Project structure

```text
apps/pipelines/
├── config/             App-level configuration: settings and Prefect block registry
├── domains/            Domain models, parsers, tasks, and flows
├── core/               Shared infrastructure: scheduler, logging, lake, and storage clients
├── providers/          Provider-specific clients and raw response models
├── dbt/                dbt Core project: Bronze -> Silver -> Gold transformations
├── docs/               Pipeline runbooks, including AWS/S3 setup
├── scripts/            SQL scripts (init_lake.sql — DuckDB/MotherDuck lake setup)
├── tests/              Unit and integration tests
├── deploy/             Docker Compose files: base, dev override, prod override
├── prefect.yaml        Prefect deployment definitions
├── pyproject.toml      Python dependencies
├── Makefile            App-level commands
└── Dockerfile          Worker image
```

Domain code is organized under `domains/<domain>/`. Add `models.py`, `parsers.py`, `tasks.py`, and `flows.py` as the domain needs them.

`config/settings.py` holds environment-variable-backed settings. `config/blocks.py` defines the Prefect block registry, which wires secrets plus non-secret infrastructure values into named Prefect blocks at startup. Tasks and flows always load credentials from the block registry at runtime, not from settings directly.

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

| Variable                | Required          | Description                                              |
| ----------------------- | ----------------- | -------------------------------------------------------- |
| `EODHD_API_KEY`         | Yes for live runs | EODHD API key.                                           |
| `MOTHERDUCK_TOKEN`      | No                | Blank uses local DuckDB; set for MotherDuck.             |
| `LOCAL_LAKE_PATH`       | No                | Local DuckDB file path when `MOTHERDUCK_TOKEN` is blank. |
| `DBT_TARGET`            | No                | dbt target name, usually `dev` locally and `prod` for MotherDuck. |
| `DBT_DUCKDB_PATH`       | No                | dbt DuckDB path, relative to the command working directory unless absolute. |
| `AWS_ACCESS_KEY_ID`     | No                | AWS access key when S3 is enabled.                       |
| `AWS_SECRET_ACCESS_KEY` | No                | AWS secret key when S3 is enabled.                       |
| `PREFECT_API_URL`       | Yes               | Prefect API URL for workers and deploy commands.         |
| `PREFECT_WORK_DIR`      | Yes               | `.` locally, `/app` in Docker.                           |
| `ENVIRONMENT`           | No                | `dev` (default), `docker_dev`, or `prod`.                |

## S3 landing zone

S3 is the landing-zone target for provider-validated raw payloads before they are parsed into typed Bronze records. Bucket names and regions are non-secret infrastructure configuration and are defined in `config/aws_resources.py`, then wired into Prefect blocks by `config/blocks.py`. AWS access keys are secrets and must stay in local `.env` files or the production deployment platform.

You do not need to create the S3 bucket and IAM user manually in the AWS Console each time. The setup is scriptable with `scripts/setup_s3_landing_zone.py`, including bucket creation, encryption, ownership controls, public-access blocking, IAM policy creation, and optional access-key generation. See [docs/aws/s3_landing_zone_guide.md](docs/aws/s3_landing_zone_guide.md) for the runbook, and [docs/aws/iam_guide.md](docs/aws/iam_guide.md) for AWS account and provisioner setup.

## Environments

| Environment     | `ENVIRONMENT` value | Prefect backend    | Lake backend        | When to use                                        |
| --------------- | ------------------- | ------------------ | ------------------- | -------------------------------------------------- |
| dev (no Docker) | `dev`               | SQLite, in-process | `LOCAL_LAKE_PATH`   | Fast Python iteration, no containers needed        |
| docker-dev      | `docker_dev`        | Postgres in Docker | `LOCAL_LAKE_PATH`   | Full stack validation, mirrors production topology |
| prod            | `prod`              | Postgres on VPS    | MotherDuck          | Live production deployment                         |

All three environments use the same `make setup` command — env vars drive which backend is targeted.

## Local development without Docker (`dev`)

Fastest feedback loop. Prefect runs in-process with a local SQLite backend; no containers required.

```bash
cd apps/pipelines
uv sync
make prefect-server        # terminal 1 — starts Prefect at http://localhost:4200
```

In a second terminal:

```bash
cd apps/pipelines
make setup                 # init lake, save blocks, create work pool, register deployments
make prefect-worker        # start the worker
```

Trigger a flow run manually:

```bash
uv run prefect deployment run 'eod-prices-daily/daily'
uv run prefect deployment run 'eod-prices-daily/backfill' -p trade_date=2026-05-09
```

## Local development with Docker (`docker_dev`)

Full stack in containers — Prefect server backed by Postgres, pipelines worker as a Docker service. Use this to validate env-var wiring and Docker image builds before deploying.

Set `ENVIRONMENT=docker_dev` in `.env`, then from `apps/pipelines/`:

```bash
cp .env.example .env
# edit .env: set ENVIRONMENT=docker_dev and any provider keys
make docker-up             # start Prefect server, Postgres, and pipelines worker
make setup                 # init lake, save blocks, create work pool, register deployments
```

Useful commands:

```bash
make docker-ps
make docker-logs-worker
make docker-down
make docker-down-volumes   # ⚠ also deletes the Postgres volume
```

Prefect UI runs at <http://localhost:4200>.

## Lake initialization

`make setup` handles this automatically. To run it standalone:

```bash
cd apps/pipelines
make lake-init                              # local DuckDB
MOTHERDUCK_TOKEN=<token> make lake-init     # MotherDuck
```

The SQL script (`scripts/init_lake.sql`) is idempotent — safe to re-run.

## dbt transformations

The dbt project lives inside this app at `dbt/`. It uses `dbt-duckdb` from the main pipelines uv environment against the same local DuckDB file in development and MotherDuck in production.

Local dbt workflow:

```bash
cd apps/pipelines
make dbt-install
make lake-init
make dbt-debug
make dbt-build
```

Focused commands:

```bash
make dbt-compile
make dbt-run
make dbt-test
make dbt-run-staging
make dbt-run-marts
```

`dbt/profiles.yml` is committed because it contains only environment-variable references, not secrets. Use `DBT_TARGET=prod` with `MOTHERDUCK_TOKEN` set to run against MotherDuck.

DuckDB allows one writer at a time. If `make lake-init` or `make dbt-build` reports a database lock, close any local DuckDB/Cursor/VS Code database viewer connected to `unique_stocks.duckdb` and rerun the command.

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
make dbt-install
make dbt-build
```

## Prefect deployments

`prefect.yaml` defines the app deployments and schedules.

```bash
cd apps/pipelines
make deploy-dry
make deploy
```

`make setup` runs this once per new environment. Use `make deploy` for subsequent deployment definition changes.

## Production (`prod`)

Production runs through Docker Compose on a VPS, with Coolify handling builds, deployments, domains, and SSL. The compose file is `apps/pipelines/deploy/docker-compose.prod.yml`, included via the root `docker-compose.yml`.

Keep these two addresses distinct — they serve different clients:

- `PREFECT_UI_API_URL`: browser-facing public URL used by the Prefect UI JavaScript app.
- `PREFECT_API_URL`: internal worker-facing URL used by the pipelines container.

Set all secrets in the deployment platform (Coolify environment variables), never in git:

- `POSTGRES_PASSWORD`
- `EODHD_API_KEY`
- `MOTHERDUCK_TOKEN`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (when S3 is enabled)
- `PREFECT_UI_API_URL`, `PREFECT_API_URL`
- `ENVIRONMENT=prod`

After the first production deploy, SSH into the VPS and run one-time setup:

```bash
cd /path/to/unique-stocks/apps/pipelines
PREFECT_API_URL=https://prefect.yourdomain.com/api \
MOTHERDUCK_TOKEN=<token> \
ENVIRONMENT=prod \
make setup
```

Re-run `make deploy` (not `make setup`) after changing deployment definitions — `setup` is only needed once per new environment.
