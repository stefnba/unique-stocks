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

Per-instrument historical backfill resume semantics (`pipeline.ingestion_coverage`,
pending-instrument rules) are documented in
[`domains/eod_price/README.md`](domains/eod_price/README.md). Fundamentals
batch-date and quota resume behavior is documented in
[`domains/fundamental/README.md`](domains/fundamental/README.md).

## Exchange reference flow

The exchange domain has two Bronze sources:

- `bronze.exchange_catalog`: provider-supported exchange/API codes, such as EODHD `US`, `LSE`, or `XETRA`.
- `bronze.exchange_mic_registry`: ISO 10383 MIC registry rows, used as the canonical exchange universe backbone.

dbt builds these into Silver exchange models. Downstream ingestion flows read provider codes from `silver.int_exchange_provider_ingestion_universe`, not directly from provider APIs or raw Bronze tables. The provider catalog remains visible there, but runtime eligibility comes from `reference.provider_namespace_policy`; catalog rows without a policy row default to non-operational `catalog_only`. Build the exchange Silver models before running flows that auto-select provider codes.

Some provider endpoint codes are valid API symbol namespaces but are not returned by the provider exchange catalog. For EODHD, `INDX` is accepted by `/exchange-symbol-list/INDX`, `/eod/GDAXI.INDX`, and `/eod-bulk-last-day/INDX`, but is omitted from `/exchanges-list`. Keep those cases in dbt seeds under `dbt/seeds/reference/` and union them into the Silver ingestion universe with `source_kind = 'curated_seed'`; do not backfill synthetic rows into `bronze.exchange_catalog`. Add or remove operational scope by editing `provider_namespace_policy`, not by changing Prefect deployment defaults.

Use `silver.int_exchange_provider_policy_resolution` to see which policy rows resolve to provider catalog or curated namespace rows before they can be used by flows.

Python flows read the Silver provider universe with purpose-specific flags. Daily price, historical price backfill, instruments, and fundamentals can have different default scopes. When Silver exists, pass explicit `provider_exchange_codes` to restrict a manual run to one or two provider namespaces.

Exchange schedule refresh is also scoped. By default it calls the provider's live
schedule-code list, intersects those codes with the dbt-built operational EOD
price universe and its MIC/operating-MIC candidates, and fetches only the overlap.
Explicit `provider_schedule_exchange_codes` are still available for narrow manual
runs. Broad provider schedule availability remains a provider lookup, not a Gold
calendar surface.

Operational order:

```text
exchange-reference-refresh
instrument-refresh and eod-price flows
```

## Local Flow Checks

Use the flow-check runner for narrow local checks instead of changing production flow defaults:

```bash
uv run pipelines-flow-check fundamental
uv run pipelines-flow-check exchange
uv run pipelines-flow-check exchange_schedule
uv run pipelines-flow-check instrument
uv run pipelines-flow-check eod-price
```

The Make target passes `FLOW` through to the flow-check runner, which owns preset validation:

```bash
make flow-check FLOW=fundamental
make flow-check FLOW=exchange
make flow-check FLOW=exchange_schedule
make flow-check FLOW=instrument
make flow-check FLOW=eod_price
```

The scoped flow-check presets (`fundamental`, `exchange_schedule`, `instrument`, and `eod_price`) pass explicit flow parameters and do not need the Silver provider universe. The `exchange` preset is broader: it refreshes the full exchange catalog and MIC registry because those reference snapshots bootstrap the exchange Silver models.

## Exchange identifiers

Providers can return several exchange-like identifiers. Keep them distinct in Python models, Bronze columns, dbt models, and logs:

| Name                              | Meaning                                                                                                                                                      | Example                       |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------- |
| `provider_exchange_code`          | Provider catalog/API code, used in endpoint paths and API symbol suffixes when the provider uses exchange-qualified API symbols.                             | `US`, `LSE`, `XETRA`          |
| `provider_code_kind`              | Silver classification for provider request codes, including exchange-backed codes, provider buckets, and curated API symbol namespaces.                      | `exchange`, `index_namespace` |
| `operating_mic_codes`             | Official MIC value or comma-separated MIC values supplied by provider metadata. Do not use this as a request code unless an endpoint explicitly asks for it. | `XNAS,XNYS`, `XLON`           |
| `provider_schedule_exchange_code` | Provider schedule/calendar endpoint code. Some values look like MICs, but this is still the provider-specific request code for that endpoint.                | `US`, `XHKG`, `XETR`          |
| `provider_listing_exchange_code`  | Exchange-like code returned on an individual instrument row from `/exchange-symbol-list/{EXCHANGE_CODE}`.                                                    | `NASDAQ`, `WAR`               |

Exchange schedule rows keep provider-supplied IANA timezone names as the source
of truth. UTC session timestamps and offsets are derived per trading date in dbt
instead of using a static offset seed or timezone API. See
[docs/exchange_timezones.md](docs/exchange_timezones.md) for the decision record.

## Project structure

```text
apps/pipelines/
├── config/             Passive settings, non-secret defaults, and identity enums
├── control_plane/      App-specific Prefect, AWS, deployment, and runtime wiring
├── orchestration/      Thin flow entrypoints and app-level dbt/workflow composition
├── core/               Reusable machinery: ingestion, HTTP, storage, lake, infrastructure/orchestration helpers
├── domains/            Domain models, tables, datasets, parsers, tasks, and services
├── providers/          Provider-specific clients and raw response models
├── lakehouse/          App-level lake schema registry and migrations
├── dbt/                dbt Core project: Bronze -> Silver -> Gold transformations
├── dashboard/          Streamlit operations dashboard for the pipeline audit schema
├── docs/               Pipeline runbooks, including AWS/S3 setup
├── scripts/            Thin grouped command entrypoints for operations
├── tests/              Unit and integration tests
├── deploy/             Docker Compose files and Dockerfiles (worker + dashboard)
├── prefect.yaml        Prefect deployment definitions
├── pyproject.toml      Python dependencies
└── Makefile            App-level commands
```

See [docs/pipeline_architecture.md](docs/pipeline_architecture.md) for folder placement rules, dependency direction, registry ownership, and the thin-flow/domain-service pattern.

`core/` stays generic and does not import concrete app providers, domains, app settings, Prefect block names, or settings-backed composition. `config/` owns passive settings and names only, including domain identity keys. Runtime wiring lives in `control_plane/`, dbt/build mappings and thin flow entrypoints live in `orchestration/`, and provider registration lives in `providers/registry.py`.

Domain code is organized under `domains/<domain>/`. Ingestion domains usually define `models.py`, `tables.py`, `datasets.py`, `parsers.py`, task modules, request/result contracts, services, plus small domain helpers when needed. See [`domains/README.md`](domains/README.md) for the domain package shape and migration convention.

### Script entrypoints

Scripts under `scripts/` are grouped command-line adapters. They own argument
parsing, environment defaults, console output, and exit codes. Reusable pipeline
behavior stays in `core/`, `orchestration/`, domain packages, providers,
control-plane modules, or config modules.

Stable operator commands are exposed in `pyproject.toml` as `pipelines-*`
entry points. See [`scripts/README.md`](scripts/README.md) before adding or
moving an entrypoint.

### File-backed lake SQL

Prefer file-backed SQL for lake reads or writes when the query has meaningful shape:
multi-CTEs, window functions, optional joins, dashboard read models, or ingestion selection logic.
Keep tiny existence checks and simple idempotency `COUNT(*)` queries inline when a separate file would add more ceremony than clarity.

SQL files live next to their Python owner and use descriptive action names such as `load_backfill_pending_instruments.sql`.
Domain SQL belongs under `domains/<domain>/sql/`; dashboard SQL belongs under `dashboard/read_models/sql/`. Python call sites should keep
orchestration and row parsing in Python while delegating the SQL body to `DataLakeClient.query_file()`, `query_one_file()`, or `execute_file()`.

SQL files may use strict Jinja templating for trusted SQL structure only:

- relation names returned by `lake.qualified_name()`
- optional predicates or joins
- generated placeholder lists such as `?, ?, ?`
- `{{ param() }}` for one DuckDB parameter marker inside a SQL file

Runtime values such as dates, provider codes, instrument codes, limits, and hashes must stay as DuckDB parameters. In SQL files, write
`{{ param() }}` instead of raw `?`; the runtime renderer turns it into `?`, while SQLFluff renders a lint-only literal so editors can parse the
template. `make sql-lint` and `make sql-format` let SQLFluff discover SQL files from the app root; add new SQL folders without touching the
Makefile. Add focused tests for non-trivial SQL files that assert the rendered query keeps important joins, filters, and parameter ordering intact.

`config/settings.py` holds environment-variable-backed settings. `control_plane/prefect/blocks.py` defines the Prefect block registry, which wires secrets plus non-secret infrastructure values into named Prefect blocks at startup. `make blocks-save` is allowed to create blank Secret/AWS blocks so they exist in the Prefect UI and can be filled in or corrected there. Tasks and flows always load credentials from the block registry at runtime, not from settings directly.

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
| `LAKE_NAME`             | Yes               | Logical lake name; used as the MotherDuck database name and local default. |
| `LOCAL_LAKE_PATH`       | No                | Local DuckDB file path; defaults to `{LAKE_NAME}.duckdb` when unset.       |
| `DBT_TARGET`            | No                | Direct dbt CLI target; app flows derive this from the active lake backend. |
| `DBT_DUCKDB_PATH`       | No                | Direct dbt CLI DuckDB path; Make/app flows derive this from the lake path. |
| `AWS_ACCESS_KEY_ID`     | No                | AWS access key when S3 is enabled.                                         |
| `AWS_SECRET_ACCESS_KEY` | No                | AWS secret key when S3 is enabled.                                         |
| `PREFECT_API_URL`       | Yes               | Prefect API URL for workers and deploy commands.                           |
| `PREFECT_WORK_DIR`      | Yes               | `.` locally, `/app` in Docker.                                             |
| `PREFECT_WORKER_LIMIT`  | No                | Worker flow-run concurrency; defaults to serialized runs.                  |
| `ENVIRONMENT`           | No                | `dev` (default), `docker_dev`, or `prod`.                                  |

## S3 landing zone

S3 is the landing-zone target for provider-validated raw payloads before they are parsed into typed Bronze records. Domain datasets use `LandingTarget` specs for raw object storage and `BronzeDataset` specs for lake writes. Bucket names, IAM names, and regions are non-secret control-plane wiring values in `control_plane/aws_resources.py` and are wired into Prefect blocks by `control_plane/prefect/blocks.py`. AWS access keys are secrets and must stay in local `.env` files or the production deployment platform.

You do not need to create the S3 bucket and IAM user manually in the AWS Console each time. The setup is scriptable with `uv run pipelines-s3-landing-zone`, including bucket creation, encryption, ownership controls, public-access blocking, IAM policy creation, and optional access-key generation. See [docs/aws/s3_landing_zone_guide.md](docs/aws/s3_landing_zone_guide.md) for the runbook, and [docs/aws/iam_guide.md](docs/aws/iam_guide.md) for AWS account and provisioner setup.

## Pipeline audit

The lake `pipeline` schema stores data-plane audit facts for ingestion and transformation runs. Prefect remains the orchestration control plane for scheduling, retries, task states, and logs; the lake audit tables answer data questions such as which exchange/date was skipped, which provider-instrument backfill failed, which landing URI produced Bronze rows, and which dbt model or test failed.

Current audit tables are defined in `lakehouse/schema.py` and applied through lake schema migrations:

- `pipeline.runs`
- `pipeline.run_units`
- `pipeline.ingestion_coverage`
- `pipeline.landing_objects`
- `pipeline.rejections`
- `pipeline.dbt_invocations`
- `pipeline.dbt_node_results`

`pipeline.ingestion_coverage` stores non-Bronze outcomes that make reruns
resume-safe, such as EOD backfill `completed` and `no_data` markers for an exact
provider-instrument/date window. See [docs/pipeline_audit.md](docs/pipeline_audit.md) for
table semantics, statuses, and the integration pattern.

## Pipeline dashboard

The Streamlit dashboard under `dashboard/` is an operational read surface for the pipeline audit schema. It lives in this app so it can reuse `config.settings` and `core.lake.DataLakeClient`, but it runs as a separate process from the Prefect worker.

Dashboard dependencies are kept in the `dashboard` optional extra. Production
deploys the dashboard service with `deploy/Dockerfile.dashboard`, which installs
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
make setup                 # migrate lake, save blocks, create work pool, upsert limits/automations, register deployments
make prefect-worker        # start the worker
```

`make prefect-worker` defaults to one concurrent flow run (`PREFECT_WORKER_LIMIT=1`) so separate
deployments do not write the same local lake at the same time. Raise it only after adding Prefect
global concurrency limits for shared lake and provider resources with `make prefect-limits`.

`make prefect-limits` upserts the shared lake writer limit and provider rate limits declared
by provider HTTP clients, such as `unique-stocks.lake-writer` for lake/dbt/migration writes and
`unique-stocks.http.provider.eodhd` for EODHD calls. The app-level limit registry is
`control_plane.prefect.limits.PREFECT_LIMITS`; provider policies still live on provider clients.
Use `ARGS="--plan"` with `make prefect-limits` and `make prefect-automations`, or
run `make deploy-plan`, when you want to read live Prefect state and preview the
planned changes without writing them. `make prefect-automations` creates event automations
for dbt failures, EOD coverage-gate gaps, stale running audit rows, ingestion partial/failure outcomes,
and cancellations. Managed event names are defined in `core.orchestration.events.PrefectEvent`, which is
also used by the emit helpers. Automations match the app, service, and `ENVIRONMENT` labels so shared Prefect
workspaces do not cross-trigger dev, docker-dev, and prod alerts. Automations use explicit no-op actions by default. Set
`SLACK_WEBHOOK_URL`, run `make blocks-save`, then rerun `make prefect-automations`
to save `BlockRegistry.SLACK_WEBHOOK` and replace those no-ops with notification actions.
If Prefect logs that a provider limit such as `unique-stocks.http.provider.eodhd` does not exist, run `make prefect-limits`
against the same `PREFECT_API_URL` used by the worker.

### Provider Call Controls

Provider-call controls exist at different layers:

| Control                                          | Scope                 | Purpose                                                                                                                          |
| ------------------------------------------------ | --------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `PREFECT_WORKER_LIMIT`                           | Worker process        | Caps concurrent flow runs on one worker. Defaults to `1`, which serializes local/dev flow execution.                             |
| Deployment `concurrency_limit` in `prefect.yaml` | One deployment        | Prevents overlapping runs of the same scheduled/manual deployment.                                                               |
| EOD backfill `batch_size`                        | One EOD backfill run  | Caps how many per-instrument EOD history requests that single run submits concurrently.                                          |
| EOD backfill `max_provider_calls`                | One EOD backfill run  | Stops that run after a fixed number of submitted provider calls, useful for daily quota or spend caps.                           |
| Provider HTTP 429 handling                       | One active flow run   | Reacts when the provider says the limit was hit, stops later batches, and leaves deferred work retryable.                        |
| Provider client `RATE_LIMIT_POLICY`              | All flows and workers | Pre-call provider-specific start-rate throttle, registered as Prefect global limits such as `unique-stocks.http.provider.eodhd`. |

With a single worker and `PREFECT_WORKER_LIMIT=1`, the global provider limit is mostly a safety net.
It becomes important when multiple provider-calling flows or workers can run at the same time.
The provider policy controls how quickly requests may start across workers. It does not hold a slot for
the full HTTP request duration, so per-flow settings such as EOD backfill `batch_size` still control
how many provider calls can be in flight inside one run.
For normal local and docker-dev work, keep `PREFECT_WORKER_LIMIT=1`; the code-owned provider and
lake limits are registered by setup without extra `.env` configuration.

#### Tuning provider `burst_capacity` for faster API calls

Provider start-rate limits are code-owned, not `.env` settings. Each provider HTTP client may
declare `RATE_LIMIT_POLICY = ProviderRateLimitPolicy(...)`. For EODHD, edit
[`providers/eodhd/client.py`](providers/eodhd/client.py):

```python
RATE_LIMIT_POLICY = ProviderRateLimitPolicy(
    burst_capacity=200,          # was 100 — more starts allowed in a burst
    slot_decay_per_second=20.0,  # was 10.0 — higher sustained starts/second
)
```

| Field                   | Effect                                                                                                                                                               |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `burst_capacity`        | Max request starts that can be issued before Prefect throttles. Raise this to shorten startup bursts (for example when a backfill fans out many concurrent fetches). |
| `slot_decay_per_second` | How quickly throttled slots refill. Raise this when sustained throughput, not just the initial burst, is the bottleneck.                                             |

After changing the policy, re-upsert Prefect global limits so workers see the new values:

```bash
make prefect-limits
```

The policy applies across all flows and workers that share the same `PREFECT_API_URL`. It does not
hold a slot for the full HTTP round-trip, so per-deployment knobs still matter: EOD backfill
`batch_size` (default `50` in `prefect.yaml`) caps concurrent in-flight calls inside one run.
Increase `burst_capacity` and `batch_size` together when you want both faster ramp-up and more
parallel requests, but stay under the provider account quota to avoid HTTP 429 deferrals.

Trigger a flow run manually:

```bash
uv run prefect deployment run 'eod-price-daily/eod-price-refresh-daily'
uv run prefect deployment run 'eod-price-daily/eod-price-backfill' -p trade_date=2026-05-09
```

## Local development with Docker (`docker_dev`)

Full stack in containers — `pipelines-server` backed by `pipelines-db`, with `pipelines-worker` as the worker service and `pipelines-dashboard` as the Streamlit audit dashboard. Use this to validate env-var wiring and Docker image builds before deploying.

Docker Compose sets `ENVIRONMENT=docker_dev` for `pipelines-worker`. Set it in `.env` too if you want host commands in the same shell to use docker-dev settings, then from `apps/pipelines/`:

```bash
cp .env.example .env
# edit .env: set ENVIRONMENT=docker_dev and any provider keys
make docker-up             # start pipelines-server, pipelines-db, worker, and dashboard
make docker-setup          # migrate container lake, save blocks, create work pool, upsert limits/automations, register deployments
```

The default docker-dev lake is isolated from the host and shared only between the `pipelines-worker` and `pipelines-dashboard` containers at `/app/lake-data/unique_stocks.duckdb`. This avoids host file-lock and path drift while still letting the dashboard inspect the worker's local audit rows. If `MOTHERDUCK_TOKEN` is set in `.env`, docker-dev intentionally targets MotherDuck instead for integration testing.
The Docker worker also defaults to `PREFECT_WORKER_LIMIT=1`; set a higher value only with matching
Prefect global concurrency limits for the shared lake and provider APIs.

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

Schema SQL files live in `lakehouse/migrations/`. Applied versions are tracked in `lake.schema_migration`; changed checksums for already-applied files are refused.
Lake migration Make targets default to `LAKE_CLI_LOG_LEVEL=WARNING` so status and generation output stays
focused; use `LAKE_CLI_LOG_LEVEL=INFO` when debugging connection/bootstrap logs.

Generate a reviewed schema diff after editing table specs:

```bash
make lake-migration                         # generate a reviewed schema diff
make lake-migration NAME="add foo column"   # optional readable filename slug
make lake-migration EMPTY=1                 # manual migration skeleton
```

`make lake-migration` compares `lakehouse/schema.py` against the connected lake, so it needs the same local DuckDB or MotherDuck access as `make lake-migrate`. If the diff contains only warnings and no executable SQL, it prints the warnings and does not create a no-op migration file.

On the first run, `make lake-migrate` applies pending migrations and then validates the live Bronze
and pipeline schemas against `lakehouse/schema.py`. Because this app is not deployed yet, older local
lakes are not upgraded in place: if validation finds drift, reset the local lake or recreate the
MotherDuck database, then rerun migrations. `make lake-migration-status` remains non-mutating and
only reports pending/applied migration files.

During greenfield schema rewrites, reset the local DuckDB lake with:

```bash
make lake-reset-local
```

For MotherDuck organization, token, CLI, and security setup, see [docs/motherduck_setup_guide.md](docs/motherduck_setup_guide.md).

## dbt transformations

The dbt project lives inside this app at `dbt/`. App-run dbt flows derive their target from the same settings as Python ingestion: local DuckDB when `MOTHERDUCK_TOKEN` is blank, MotherDuck when it is set. Direct dbt CLI commands can still use `DBT_TARGET`, `LAKE_NAME`, and `DBT_DUCKDB_PATH`.

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
make dbt-docs-generate
make dbt-docs-serve
make dbt-docs
```

`make dbt-docs` generates and serves the local dbt documentation site, including the model/source lineage graph and live warehouse column types. Close local DuckDB viewers such as TablePlus before running it, because dbt needs to inspect the warehouse catalog. Run `make dbt-build` or `make dbt-seed` first if you need seed column types. Override `DBT_DOCS_PORT` if port `8080` is already in use.

For dbt sources, naming, mart-shape, key, lineage, testing, and materialization conventions, see [`dbt/README.md#conventions`](dbt/README.md#conventions).

`dbt/profiles.yml` is committed because it contains only environment-variable references, not secrets. Use `DBT_TARGET=prod` with `MOTHERDUCK_TOKEN` set to run against MotherDuck.

Production dbt execution is available as Prefect deployments:

- `dbt-build/ingestion-control-build`: Silver runtime contract models used by Python ingestion for provider universes, coverage gates, and backfill selectors.
- `dbt-build/exchange-build`: exchange staging/intermediate + exchange marts/provider ingestion universe, including `provider_namespace_policy`.
- `dbt-build/instrument-build`: instrument staging/intermediate + instrument dimension.
- `dbt-build/price-build`: full price staging/intermediate models and price marts.
- `dbt-build/fundamental-build`: fundamental staging/intermediate + fundamental/instrument marts.

Ingestion deployments set `run_dbt_build=true` where Silver/Gold freshness matters. A clean
domain audit status (`pipeline.runs.status = 'completed'`) launches the matching dbt deployment
with the ingestion `run_id` as `parent_run_id`; `partial`, `failed`, or all-skipped ingestion
runs do not auto-promote Bronze data. Run the dbt deployments directly for bootstrap, repair,
or full rebuilds. Each dbt invocation writes to its own `dbt/target/pipeline-runs/<dbt_run_id>/`
directory; orchestration reads `run_results.json` only from that per-run path, records the exact
artifact path in the dbt audit table, and publishes a Prefect Markdown artifact with the invocation
summary. Every ingestion flow publishes a compact Prefect Markdown summary artifact, and EOD
coverage gates publish a Prefect table artifact when they find gaps. Successful Bronze writes and
dbt build/run invocations record simple Prefect asset materializations for Bronze, Silver, and
Gold layer visibility in Prefect.
If a post-ingestion dbt deployment fails after the ingestion audit has completed, the parent Prefect
flow intentionally fails for alerting while `pipeline.runs` keeps the ingestion status and dbt audit
tables carry the transformation failure details.
The shared dbt build deployment uses a one-run queue, so overlapping clean ingestion runs wait for
the active transform instead of cancelling a newer promotion.

DuckDB allows one writer at a time. If `make lake-migrate` or `make dbt-build` reports a database lock, close any local DuckDB/Cursor/VS Code database viewer connected to `unique_stocks.duckdb` and rerun the command.

## Quality checks

Run the full app quality gate before committing pipeline changes:

```bash
cd apps/pipelines
make check
```

Run the exact GitHub CI quality path locally:

```bash
make ci-local
```

Focused commands:

```bash
make ci-local
make test
make test-unit
make test-integration
make lint
make lint-fix
make format
make format-check
make text-format
make text-format-check
make sql-lint
make sql-format
make typecheck
make dbt-install
make dbt-build
```

## Prefect deployments

`prefect.yaml` registers one domain-first deployment name per operational mode (scheduled, manual, backfill, build).
Each mode maps to the same orchestration flow with different default parameters.

| Domain                     | Deployments                                                                                                                                                                                                                                                                                        | Mode                                |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| EOD price (bulk)           | `eod-price-daily/eod-price-refresh-daily`, `eod-price-daily/eod-price-backfill`                                                                                                                                                                                                                    | scheduled, backfill                 |
| EOD price (per-instrument) | `eod-price-backfill/eod-price-historical-backfill`                                                                                                                                                                                                                                                 | backfill                            |
| Exchange reference         | `exchange-reference-refresh/exchange-reference-refresh-monthly`, `exchange-reference-refresh/exchange-reference-refresh-manual`; leaf troubleshooting deployments `exchange-catalog-refresh/exchange-catalog-refresh-manual`, `exchange-mic-registry-refresh/exchange-mic-registry-refresh-manual` | scheduled, bootstrap, manual        |
| Exchange schedule          | `exchange-schedule-refresh/exchange-schedule-refresh-weekly`, `exchange-schedule-refresh/exchange-schedule-refresh-manual`                                                                                                                                                                         | scheduled, manual                   |
| Instrument                 | `instrument-refresh/instrument-refresh-weekly`, `instrument-refresh/instrument-refresh-manual`                                                                                                                                                                                                     | scheduled, manual                   |
| Fundamental                | `fundamental-quarterly/fundamental-refresh-quarterly`, `fundamental-quarterly/fundamental-refresh-manual`, `fundamental-quarterly/fundamental-backfill`, `fundamental-quarterly/fundamental-replay`                                                                                                | scheduled, manual, backfill, replay |
| dbt                        | `dbt-build/ingestion-control-build`, `dbt-build/exchange-build`, `dbt-build/instrument-build`, `dbt-build/price-build`, `dbt-build/fundamental-build`                                                                                                                                              | build                               |

Bootstrap order for a new environment: exchange reference manual → ingestion-control-build when selector/control views are needed → instrument → ingest. The reference parent runs catalog, MIC, exchange-build, scoped schedule refresh, and the final exchange-build in order. In normal operation, scheduled exchange reference, exchange schedule, instrument, EOD price, and fundamentals deployments trigger their matching dbt build after a clean audit status; pass `run_dbt_build=false` for Bronze-only runs. Historical EOD backfill also has a preflight guard: the deployment sets `build_selection_views_if_missing=true`, so it runs `dbt-build/ingestion-control-build` before provider-instrument selection when the required Silver selector views do not exist yet.

`make deploy` is the source-of-truth sync: it removes orphaned deployments owned by this app
(entrypoints under `orchestration.*`), then applies `prefect.yaml`.
Manual UI experiments with unrelated entrypoints are left untouched.

```bash
cd apps/pipelines
make deploy-plan       # preview prefect.yaml entries + orphan deletions
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

Set production environment variables in the deployment platform (Coolify environment variables), never in git:

- `POSTGRES_PASSWORD`
- Provider API key(s) from `.env.example`
- `MOTHERDUCK_TOKEN`
- `LAKE_NAME=unique_stocks` unless production should target a different MotherDuck database
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (when S3 is enabled)
- `PREFECT_UI_API_URL`, `PREFECT_API_URL`
- `ENVIRONMENT=prod`

Optional production infrastructure configuration:

- `PREFECT_HOST_BIND_IP` — defaults to `127.0.0.1`; use `0.0.0.0` only behind a protected reverse proxy.
- `DASHBOARD_HOST_BIND_IP` — defaults to `127.0.0.1`; use `0.0.0.0` only behind a protected reverse proxy.
- `DASHBOARD_MOTHERDUCK_TOKEN` — optional dashboard-specific MotherDuck token. Prefer a read-only token here; when omitted, the dashboard falls back to `MOTHERDUCK_TOKEN`.
- `PREFECT_UI_URL` — optional browser-facing Prefect UI base URL used for run deep links from the dashboard.
- `PREFECT_WORKER_LIMIT` — maximum concurrent flow runs per worker process; defaults to `1`.
- `SLACK_WEBHOOK_URL` — optional Slack incoming webhook URL saved by `make blocks-save` as `BlockRegistry.SLACK_WEBHOOK`. When set, event automations send Slack notifications; when blank, they use explicit no-op actions.
- `OPERATIONAL_HEALTH_RECENT_DOMAINS` — optional comma- or whitespace-separated domains the deployed `pipelines-operational-health` container must see recently, for example `eod_price,fundamental`.
- `OPERATIONAL_HEALTH_RECENT_HOURS` — freshness window for configured recent domains; defaults to `36`.
- `OPERATIONAL_HEALTH_STALE_RUNNING_HOURS` — stale-running threshold; defaults to `2`.
- `OPERATIONAL_HEALTH_LAKE_READ_ONLY` — defaults to `auto`; leave it there for regular MotherDuck tokens, or set `true` when the monitor uses a MotherDuck read-scaling token.

Production fails closed when `ENVIRONMENT=prod` is set without `MOTHERDUCK_TOKEN`; set the token or use `ENVIRONMENT=dev` for local work.
Blank Prefect blocks may still be saved during setup; update provider and AWS block values in the Prefect UI before running live provider or S3-backed flows.

Production applies pending lake migrations through the one-shot `pipelines-migrator` service before workers start. This
keeps deploys with schema changes from consuming work against an old lake schema and avoids every worker racing to run
migrations when the worker service is scaled out. `pipelines-migrator` is expected to exit successfully after applying
migrations; `make setup` still runs migrations idempotently as part of first-time bootstrap.

Workers are serialized by default with `PREFECT_WORKER_LIMIT=1`. Keep deployment-level concurrency
limits in place, and keep the `make prefect-limits` global limits in sync before scaling worker
concurrency for shared lake files, MotherDuck writes, or provider quotas. Local DuckDB should keep
one lake writer slot. The lake writer slot policy is declared in
`control_plane/prefect/limits.py`; raise it only after validating concurrent lake, dbt, migration,
and audit writes in the target environment.

Advanced Prefect limit overrides are intentionally not part of `.env.example`.
`PREFECT_GLOBAL_LIMITS_FAIL_CLOSED=true` can make missing Prefect limits fail closed after the environment
has been bootstrapped with `make prefect-limits`.

Docker healthchecks are intentionally container-local. The worker healthcheck verifies that the container can reach the
Prefect API and that a Prefect worker process is running; it does not prove that scheduled ingestion is fresh or that a
specific work pool is consuming every expected deployment.

The deployed `pipelines-operational-health` service converts the operational health command into a Docker health status
that Coolify, Docker, or a container-aware uptime monitor can watch. By default it checks Prefect reachability, the
presence of `pipeline.runs`, and stale running rows. Set `OPERATIONAL_HEALTH_RECENT_DOMAINS` in production to add
freshness checks for the domains you care about. Domain freshness requires recent `completed` runs; `partial` and
`skipped` runs remain visible in the dashboard but do not satisfy the production health gate:

```bash
OPERATIONAL_HEALTH_RECENT_DOMAINS=eod_price,fundamental
OPERATIONAL_HEALTH_RECENT_HOURS=36
```

`OPERATIONAL_HEALTH_LAKE_READ_ONLY=auto` uses a normal SELECT-only connection when `MOTHERDUCK_TOKEN` is set because
regular MotherDuck tokens cannot be opened through DuckDB's read-only mode. Set it to `true` only when the monitor token
is a MotherDuck read-scaling token.

You can still run the same check by hand when investigating an incident:

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

This assumes `MOTHERDUCK_TOKEN` and `LAKE_NAME` are already present in the deployment environment. The setup command
saves Prefect blocks, creates/updates the work pool, upserts limits and automations, and registers deployments.

Re-run `make deploy` (not `make setup`) after changing deployment definitions — `setup` is only needed once per new environment.
