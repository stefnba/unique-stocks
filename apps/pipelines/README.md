# Pipelines

`apps/pipelines` is the Prefect 3 ingestion app for Unique Stocks. It fetches market data from providers, validates provider responses, writes typed Bronze records through the lake client, coordinates scheduled runs, and includes the pipeline audit dashboard for operational monitoring.

The active path is end-of-day price ingestion for the configured market data provider. Exchange and instrument are reference flows; fundamental ingests full provider documents plus typed stock, ETF, mutual fund, and index Bronze slices for the fixture-backed EODHD shapes.

## Status

| Domain        | Status         | Schedule                                           |
| ------------- | -------------- | -------------------------------------------------- |
| `eod_price`   | Active         | Weekdays after market close, plus manual backfill. |
| `exchange`    | Reference flow | Manual or monthly.                                 |
| `instrument`  | Reference flow | Weekly.                                            |
| `fundamental` | Active v1      | Manual or quarterly.                               |

Per-ticker historical backfill resume semantics (`pipeline.ingestion_coverage`,
pending-symbol rules) are documented in
[`domains/eod_price/README.md`](domains/eod_price/README.md). Fundamentals
batch-date and quota resume behavior is documented in
[`domains/fundamental/README.md`](domains/fundamental/README.md).

## Exchange reference flow

The exchange domain has two Bronze sources:

- `bronze.exchange_catalog`: provider-supported exchange/API codes, such as EODHD `US`, `LSE`, or `XETRA`.
- `bronze.exchange_mic_registry`: ISO 10383 MIC registry rows, used as the canonical exchange universe backbone.

dbt builds these into Silver exchange models. Downstream ingestion flows read provider codes from `silver.int_exchange_provider_ingestion_universe`, not directly from provider APIs or raw Bronze tables. Build the exchange Silver models before running flows that auto-select provider codes.

Some provider endpoint codes are valid symbol namespaces but are not returned by the provider exchange catalog. For EODHD, `INDX` is accepted by `/exchange-symbol-list/INDX`, `/eod/GDAXI.INDX`, and `/eod-bulk-last-day/INDX`, but is omitted from `/exchanges-list`. Keep those cases in dbt seeds under `dbt/seeds/reference/` and union them into the Silver ingestion universe with `source_kind = 'curated_seed'`; do not backfill synthetic rows into `bronze.exchange_catalog`.

Python flows read the Silver provider universe with purpose-specific flags. When Silver exists, pass explicit `provider_exchange_codes` to restrict a manual run to one or two provider namespaces.

Operational order:

```text
exchange-catalog-refresh
exchange-mic-registry-refresh
dbt-build/exchange-build
instrument-refresh and eod-price flows
```

## Local Smoke Runs

Use the smoke runner for narrow local checks instead of changing production flow defaults:

```bash
uv run python scripts/run_smoke.py fundamental
uv run python scripts/run_smoke.py exchange
uv run python scripts/run_smoke.py exchange_schedule
uv run python scripts/run_smoke.py instrument
uv run python scripts/run_smoke.py eod-price
```

The Make target passes `FLOW` through to the smoke runner, which owns preset validation:

```bash
make smoke FLOW=fundamental
make smoke FLOW=exchange
make smoke FLOW=exchange_schedule
make smoke FLOW=instrument
make smoke FLOW=eod_price
```

The scoped smoke presets (`fundamental`, `exchange_schedule`, `instrument`, and `eod_price`) pass explicit flow parameters and do not need the Silver provider universe. The `exchange` preset is broader: it refreshes the full exchange catalog and MIC registry because those reference snapshots bootstrap the exchange Silver models.

## Exchange identifiers

Providers can return several exchange-like identifiers. Keep them distinct in Python models, Bronze columns, dbt models, and logs:

| Name                              | Meaning                                                                                                                                                      | Example                       |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------- |
| `provider_exchange_code`          | Provider catalog/API code, used in endpoint paths and ticker suffixes when the provider uses exchange-qualified symbols.                                     | `US`, `LSE`, `XETRA`          |
| `provider_code_kind`              | Silver classification for provider request codes, including exchange-backed codes, provider buckets, and curated symbol namespaces.                          | `exchange`, `index_namespace` |
| `operating_mic_codes`             | Official MIC value or comma-separated MIC values supplied by provider metadata. Do not use this as a request code unless an endpoint explicitly asks for it. | `XNAS,XNYS`, `XLON`           |
| `provider_schedule_exchange_code` | Provider schedule/calendar endpoint code. Some values look like MICs, but this is still the provider-specific request code for that endpoint.                | `US`, `XHKG`, `XETR`          |
| `provider_listing_exchange_code`  | Exchange-like code returned on an individual instrument row from `/exchange-symbol-list/{EXCHANGE_CODE}`.                                                    | `NASDAQ`, `WAR`               |

## Project structure

```text
apps/pipelines/
├── config/             App-level configuration: settings and Prefect block registry
├── domains/            Domain models, tables, datasets, parsers, tasks, and flows
├── core/               Shared infrastructure: ingestion, logging, lake, and storage clients
├── providers/          Provider-specific clients and raw response models
├── dbt/                dbt Core project: Bronze -> Silver -> Gold transformations
├── dashboard/          Streamlit operations dashboard for the pipeline audit schema
├── docs/               Pipeline runbooks, including AWS/S3 setup
├── scripts/            Local operational helpers
├── tests/              Unit and integration tests
├── deploy/             Docker Compose files: base, dev override, prod override
├── prefect.yaml        Prefect deployment definitions
├── pyproject.toml      Python dependencies
├── Makefile            App-level commands
├── Dockerfile          Worker image
└── Dockerfile.dashboard Streamlit dashboard image
```

Domain code is organized under `domains/<domain>/`. Ingestion domains usually define `models.py`, `tables.py`, `datasets.py`, `parsers.py`, task modules, and `flows.py`, plus small domain helpers when needed.

`config/settings.py` holds environment-variable-backed settings. `config/blocks.py` defines the Prefect block registry, which wires secrets plus non-secret infrastructure values into named Prefect blocks at startup. Tasks and flows always load credentials from the block registry at runtime, not from settings directly.

## Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/)
- Docker Desktop or OrbStack for Docker-based local infrastructure
- A market data provider API key for live provider runs

## Environment

Create local configuration from the example file:

```bash
cd apps/pipelines
cp .env.example .env
```

Set at least the active provider API key shown in `.env.example` for live provider runs. Leave `MOTHERDUCK_TOKEN` blank to use local DuckDB.

| Variable                | Required          | Description                                                                |
| ----------------------- | ----------------- | -------------------------------------------------------------------------- |
| Provider API key(s)     | Yes for live runs | Current market data provider credentials; see `.env.example`.              |
| `MOTHERDUCK_TOKEN`      | No                | Blank uses local DuckDB; set for MotherDuck.                               |
| `LOCAL_LAKE_PATH`       | No                | Local DuckDB file path when `MOTHERDUCK_TOKEN` is blank.                   |
| `DBT_TARGET`            | No                | Direct dbt CLI target; app flows derive this from the active lake backend. |
| `DBT_DUCKDB_PATH`       | No                | Direct dbt CLI DuckDB path; app flows derive this from `LOCAL_LAKE_PATH`.  |
| `AWS_ACCESS_KEY_ID`     | No                | AWS access key when S3 is enabled.                                         |
| `AWS_SECRET_ACCESS_KEY` | No                | AWS secret key when S3 is enabled.                                         |
| `PREFECT_API_URL`       | Yes               | Prefect API URL for workers and deploy commands.                           |
| `PREFECT_WORK_DIR`      | Yes               | `.` locally, `/app` in Docker.                                             |
| `ENVIRONMENT`           | No                | `dev` (default), `docker_dev`, or `prod`.                                  |

## S3 landing zone

S3 is the landing-zone target for provider-validated raw payloads before they are parsed into typed Bronze records. Domain datasets use `LandingTarget` specs for raw object storage and `BronzeDataset` specs for lake writes. Bucket names and regions are non-secret infrastructure configuration and are defined in `config/aws_resources.py`, then wired into Prefect blocks by `config/blocks.py`. AWS access keys are secrets and must stay in local `.env` files or the production deployment platform.

You do not need to create the S3 bucket and IAM user manually in the AWS Console each time. The setup is scriptable with `scripts/setup_s3_landing_zone.py`, including bucket creation, encryption, ownership controls, public-access blocking, IAM policy creation, and optional access-key generation. See [docs/aws/s3_landing_zone_guide.md](docs/aws/s3_landing_zone_guide.md) for the runbook, and [docs/aws/iam_guide.md](docs/aws/iam_guide.md) for AWS account and provisioner setup.

## Pipeline audit

The lake `pipeline` schema stores data-plane audit facts for ingestion and transformation runs. Prefect remains the orchestration control plane for scheduling, retries, task states, and logs; the lake audit tables answer data questions such as which exchange/date was skipped, which ticker backfill failed, which landing URI produced Bronze rows, and which dbt model or test failed.

Current audit tables are defined in `lake/schema.py` and applied through lake schema migrations:

- `pipeline.runs`
- `pipeline.run_units`
- `pipeline.ingestion_coverage`
- `pipeline.landing_objects`
- `pipeline.rejections`
- `pipeline.dbt_invocations`
- `pipeline.dbt_node_results`

`pipeline.ingestion_coverage` stores terminal non-Bronze outcomes that make
reruns resume-safe, such as EOD backfill `no_data` for an exact ticker/date
range. See [docs/pipeline_audit.md](docs/pipeline_audit.md) for table
semantics, statuses, and the integration pattern.

## Pipeline dashboard

The Streamlit dashboard under `dashboard/` is an operational read surface for the pipeline audit schema. It lives in this app so it can reuse `config.settings` and `core.clients.lake.DataLakeClient`, but it runs as a separate process from the Prefect worker.

Dashboard dependencies are kept in the `dashboard` optional extra. Production
deploys the dashboard service with `Dockerfile.dashboard`, which installs
`--extra dashboard`; the Prefect worker image intentionally installs only base
pipeline dependencies.

Run it locally from `apps/pipelines/`:

```bash
make dashboard
```

The dashboard reads `pipeline.runs` for summary health and loads drill-down details from `pipeline.run_units`, `pipeline.landing_objects`, `pipeline.rejections`, and the dbt audit tables. Set `MOTHERDUCK_TOKEN` to inspect MotherDuck; otherwise it reads `LOCAL_LAKE_PATH`.

Run detail pages are routed with `?page=run&run_id=<run_id>`. Use the `Open`
links in the run tables to jump from the overview into one run's units, landing
objects, rejections, and dbt results.

## Operational logging

Pipeline code uses `structlog` through the central configuration in `core/utils/logging.py`. Development and
docker-dev default to readable console logs; production defaults to JSON logs on stdout so Prefect and the container
platform can collect them. Prefect captures app package logs through `PREFECT_LOGGING_EXTRA_LOGGERS`.

Logging is configured when the `core` package is first imported. Set `ENVIRONMENT`, `PIPELINE_LOG_FORMAT`,
`PIPELINE_LOG_LEVEL`, and optional `GIT_SHA` before importing flow/task modules in local scripts or workers. Compose
sets these before the worker imports app code.

Do not write general log lines to the lake or S3. Use Prefect/stdout logs for narrative debugging, `pipeline.*` audit
tables for durable run facts and dashboard queries, and S3 only for raw provider landing payloads.

## Environments

| Environment     | `ENVIRONMENT` value | Prefect backend    | Lake backend      | When to use                                        |
| --------------- | ------------------- | ------------------ | ----------------- | -------------------------------------------------- |
| dev (no Docker) | `dev`               | SQLite, in-process | `LOCAL_LAKE_PATH` | Fast Python iteration, no containers needed        |
| docker-dev      | `docker_dev`        | Postgres in Docker | `LOCAL_LAKE_PATH` | Full stack validation, mirrors production topology |
| prod            | `prod`              | Postgres on VPS    | MotherDuck        | Live production deployment                         |

Host development uses `make setup`. Docker development uses `make docker-setup` so lake initialization and deployment registration run inside the `pipelines-worker` container.

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
make setup                 # migrate lake, save blocks, create work pool, register deployments
make prefect-worker        # start the worker
```

Trigger a flow run manually:

```bash
uv run prefect deployment run 'eod-price-daily/daily'
uv run prefect deployment run 'eod-price-daily/backfill' -p trade_date=2026-05-09
```

## Local development with Docker (`docker_dev`)

Full stack in containers — `pipelines-server` backed by `pipelines-db`, with `pipelines-worker` as the worker service and `pipelines-dashboard` as the Streamlit audit dashboard. Use this to validate env-var wiring and Docker image builds before deploying.

Docker Compose sets `ENVIRONMENT=docker_dev` for `pipelines-worker`. Set it in `.env` too if you want host commands in the same shell to use docker-dev settings, then from `apps/pipelines/`:

```bash
cp .env.example .env
# edit .env: set ENVIRONMENT=docker_dev and any provider keys
make docker-up             # start pipelines-server, pipelines-db, worker, and dashboard
make docker-setup          # migrate container lake, save blocks, create work pool, register deployments
```

The default docker-dev lake is isolated from the host and shared only between the `pipelines-worker` and `pipelines-dashboard` containers at `/app/lake-data/unique_stocks.duckdb`. This avoids host file-lock and path drift while still letting the dashboard inspect the worker's local audit rows. If `MOTHERDUCK_TOKEN` is set in `.env`, docker-dev intentionally targets MotherDuck instead for integration testing.

Useful commands:

```bash
make docker-ps
make docker-logs-worker
make docker-logs-dashboard
make docker-down
make docker-down-volumes   # ⚠ also deletes the Postgres volume
```

Prefect UI runs at <http://localhost:4200>.
The pipeline audit dashboard runs at <http://localhost:8501>.

## Lake schema migrations

`make setup` handles this automatically. To run migrations standalone:

```bash
cd apps/pipelines
make lake-migration-status                  # preview pending/applied migrations without changing the lake
make lake-migrate                           # local DuckDB, or MotherDuck when MOTHERDUCK_TOKEN is set
```

For docker-dev, use `make docker-setup` instead so migrations run in the same container filesystem as the worker.

Schema SQL files live in `lake/migrations/`. Applied versions are tracked in `lake.schema_migration`; changed checksums for already-applied files are refused.

Generate a reviewed schema diff after editing table specs:

```bash
make lake-migration                         # generate a reviewed schema diff
make lake-migration NAME="add foo column"   # optional readable filename slug
make lake-migration EMPTY=1                 # manual migration skeleton
```

`make lake-migration` compares `lake/schema.py` against the connected lake, so it needs the same local DuckDB or MotherDuck access as `make lake-migrate`. If the diff contains only warnings and no executable SQL, it prints the warnings and does not create a no-op migration file.

On the first run against an older lake that does not have `lake.schema_migration`, `make lake-migrate` treats every migration file as pending. The initial migration is written with `IF NOT EXISTS` DDL so it can bootstrap tracking and record checksums for an existing local lake.

During greenfield schema rewrites, reset the local DuckDB lake with:

```bash
make lake-reset-local
```

For MotherDuck organization, token, CLI, and security setup, see [docs/motherduck_setup_guide.md](docs/motherduck_setup_guide.md).

## dbt transformations

The dbt project lives inside this app at `dbt/`. App-run dbt flows derive their target from the same settings as Python ingestion: local DuckDB when `MOTHERDUCK_TOKEN` is blank, MotherDuck when it is set. Direct dbt CLI commands can still use `DBT_TARGET` and `DBT_DUCKDB_PATH`.

Local dbt workflow:

```bash
cd apps/pipelines
make dbt-install
make lake-migrate
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

Production dbt execution is also available as Prefect deployments:

- `dbt-build/exchange-build`: exchange staging/intermediate + provider ingestion universe.
- `dbt-build/price-build`: price staging and mart models.
- `dbt-build/fundamental-build`: fundamental staging models.

Both price and exchange builds read `dbt/target/run_results.json` and write dbt audit rows to the lake.

DuckDB allows one writer at a time. If `make lake-migrate` or `make dbt-build` reports a database lock, close any local DuckDB/Cursor/VS Code database viewer connected to `unique_stocks.duckdb` and rerun the command.

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

`prefect.yaml` registers one deployment per operational mode (scheduled, manual, backfill, build).
Each mode maps to the same domain flow with different default parameters.

| Domain                 | Deployments                                                               | Mode                     |
| ---------------------- | ------------------------------------------------------------------------- | ------------------------ |
| EOD price (bulk)       | `eod-price-daily/daily`, `/backfill`                                      | scheduled, backfill      |
| EOD price (per-ticker) | `eod-price-backfill/historical-backfill`                                  | backfill                 |
| Exchange catalog / MIC | `exchange-catalog-refresh/manual`, `exchange-mic-registry-refresh/manual` | bootstrap                |
| Exchange schedule      | `exchange-schedule-refresh/manual`                                        | manual                   |
| Instrument             | `instrument-refresh/weekly`, `/manual`                                    | scheduled, manual        |
| Fundamental            | `fundamental-quarterly/manual`, `/backfill`, `/replay`                    | manual, backfill, replay |
| dbt                    | `dbt-build/exchange-build`, `/price-build`, `/fundamental-build`          | build                    |

Bootstrap order for a new environment: exchange manual → exchange-build → instrument → ingest → matching `*-build`.

`make deploy` is the source-of-truth sync: it removes orphaned deployments owned by this app
(entrypoints under `domains.*` or `core.transforms.*`), then applies `prefect.yaml`.
Manual UI experiments with unrelated entrypoints are left untouched.

```bash
cd apps/pipelines
make deploy-dry        # preview manifest + orphan deletions
make deploy            # prune orphans, then apply prefect.yaml
make deploy-upsert     # apply only — keep orphaned app deployments
```

`make setup` runs `make deploy` once per new environment.

## Production (`prod`)

Production runs through Docker Compose on a VPS, with Coolify handling builds, deployments, domains, and SSL. The compose file is `apps/pipelines/deploy/docker-compose.prod.yml`, included via the root `docker-compose.yml`.

Keep these two addresses distinct — they serve different clients:

- `PREFECT_UI_API_URL`: browser-facing public URL used by the Prefect UI JavaScript app.
- `PREFECT_API_URL`: internal worker-facing URL used by the `pipelines-worker` container.

The production compose file binds the host Prefect port to `127.0.0.1` by default. Keep that default when a local
reverse proxy can reach the service on the VPS. Set `PREFECT_HOST_BIND_IP=0.0.0.0` only when the deployment platform
requires a public host bind, and put the Prefect UI/API behind TLS plus access control.

Set all secrets in the deployment platform (Coolify environment variables), never in git:

- `POSTGRES_PASSWORD`
- Provider API key(s) from `.env.example`
- `MOTHERDUCK_TOKEN`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (when S3 is enabled)
- `PREFECT_UI_API_URL`, `PREFECT_API_URL`
- `ENVIRONMENT=prod`

Optional production infrastructure configuration:

- `PREFECT_HOST_BIND_IP` — defaults to `127.0.0.1`; use `0.0.0.0` only behind a protected reverse proxy.
- `DASHBOARD_HOST_BIND_IP` — defaults to `127.0.0.1`; use `0.0.0.0` only behind a protected reverse proxy.
- `DASHBOARD_MOTHERDUCK_TOKEN` — optional dashboard-specific MotherDuck token. Prefer a read-only token here; when omitted, the dashboard falls back to `MOTHERDUCK_TOKEN`.
- `PREFECT_UI_URL` — optional browser-facing Prefect UI base URL used for run deep links from the dashboard.

Production fails closed when `ENVIRONMENT=prod` is set without `MOTHERDUCK_TOKEN`; set the token or use `ENVIRONMENT=dev` for local work.

The production worker applies pending lake migrations before it starts the Prefect worker process. This keeps a deploy
with schema changes from consuming work against an old lake schema. `make setup` still runs migrations idempotently as
part of first-time bootstrap.

Docker healthchecks are intentionally container-local. The worker healthcheck verifies that the container can reach the
Prefect API and that a Prefect worker process is running; it does not prove that scheduled ingestion is fresh or that a
specific work pool is consuming every expected deployment. Use the operational health command for that:

```bash
cd apps/pipelines
make operational-health
make operational-health ARGS="--recent-domain eod_price --recent-domain fundamental --recent-hours 36"
```

`operational-health` fails when Prefect is unreachable, `pipeline.runs` is missing, a run is stuck in `running` beyond
the stale threshold, or a required domain has no recent terminal run. Tune `--stale-running-hours`, `--recent-domain`,
and `--recent-hours` to match the production schedule you are monitoring.

After the first production deploy, SSH into the VPS and run one-time setup:

```bash
cd /path/to/unique-stocks/apps/pipelines
PREFECT_API_URL=https://prefect.yourdomain.com/api \
ENVIRONMENT=prod \
make setup
```

This assumes `MOTHERDUCK_TOKEN` is already present in the deployment environment. The setup command saves Prefect blocks,
creates/updates the work pool, and registers deployments.

Re-run `make deploy` (not `make setup`) after changing deployment definitions — `setup` is only needed once per new environment.
