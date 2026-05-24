# Unique Stocks

Unique Stocks is a self-hosted financial data platform for finding attractive investment opportunities across markets. The project ingests market data, transforms it into analytics-ready tables, and will serve it through a lightweight studio for search, charts, and watchlists.

The goal is a lean, reliable stack that can run cheaply on a single VPS while still keeping the data model clean enough to grow.

## Current status

| Area                 | Status               | Notes                                                          |
| -------------------- | -------------------- | -------------------------------------------------------------- |
| `apps/pipelines`     | Active               | Prefect 3 ingestion app for EODHD market data.                 |
| `dbt_project`        | Planned / scaffolded | dbt Core project for Bronze -> Silver -> Gold transformations. |
| `apps/api`           | Not implemented      | API boundary is still an open decision.                        |
| `apps/studio`        | Not implemented      | Studio stack is still an open decision.                        |
| `docker-compose.yml` | Active               | Root compose entrypoint — includes per-app stacks.             |

## Architecture

The v1 platform has three responsibilities:

1. Ingest raw provider data through scheduled Python / Prefect flows.
2. Transform typed lake data through dbt models.
3. Serve clean gold-layer data through a future app surface.

Data flow:

```text
External APIs
  -> apps/pipelines
  -> S3 landing zone
  -> MotherDuck bronze
  -> dbt silver/gold
  -> apps/api and/or apps/studio
```

S3 is the replayable landing zone for raw provider responses. MotherDuck is the primary query store for Bronze, Silver, and Gold schemas. dbt owns transformation logic after Bronze.

## Repository layout

```text
unique-stocks/
├── apps/
│   ├── pipelines/      Prefect ingestion app
│   ├── api/            Planned API app
│   └── studio/         Planned user-facing studio
├── dbt_project/        dbt Core transformations
├── docker-compose.yml  Root compose entrypoint (includes per-app stacks)
├── AGENTS.md           Coding-agent instructions
├── PLAN.md             Product and architecture plan
├── Makefile            Monorepo command entrypoint
└── README.md           Project overview
```

Every folder under `apps/` has its own README with standalone app status and run guidance.

## Quick start

For the active pipelines app (docker-dev):

```bash
cp apps/pipelines/.env.example apps/pipelines/.env
# edit .env: set ENVIRONMENT=docker_dev and provider keys
make infra-up          # start Prefect server, Postgres, and worker
make pipelines-setup   # init lake, save blocks, create work pool, register deployments
```

Prefect UI runs at <http://localhost:4200>.

For local Python development without Docker:

```bash
cd apps/pipelines
uv sync
make prefect-server   # terminal 1
make setup            # terminal 2 — init lake, save blocks, create pool, deploy
make prefect-worker   # terminal 2 — start the worker
```

See [apps/pipelines/README.md](apps/pipelines/README.md) for the full pipelines runbook.

## Common commands

From the repository root:

```bash
make help
make pipelines-check
make pipelines-test
make infra-up
make infra-logs-pipelines
make infra-down
make dbt-compile
make dbt-test
```

From `apps/pipelines/`:

```bash
make check
make test
make lint
make typecheck
make deploy-dry
```

## Documentation map

- [PLAN.md](PLAN.md): v1 product scope, architecture decisions, roadmap, and open decisions.
- [AGENTS.md](AGENTS.md): coding conventions and guardrails for AI/coding agents.
- [apps/pipelines/README.md](apps/pipelines/README.md): pipelines setup, configuration, Prefect usage, testing, and deployment notes.
- [apps/api/README.md](apps/api/README.md): API placeholder and ownership notes.
- [apps/studio/README.md](apps/studio/README.md): studio placeholder and stack decision notes.

## Secrets

Do not commit secrets. Local configuration belongs in `.env` files copied from `.env.example`.
