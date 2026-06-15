# Agent Guidelines for Pipelines

## Python conventions

- Use type hints on every function signature.
- Use Pydantic v2 models for all external data.
- Set `extra="forbid"` on external API models to catch provider drift early.
- Use `async` / `await` throughout provider and orchestration paths.
- Use `httpx` async clients for HTTP.
- Keep provider fetches, landing writes, parsing, and Bronze writes in separate tasks.
- Keep Prefect flows thin: parameter normalization, domain service calls, and app-level post-ingestion orchestration only.
- Put sequencing, decisions, counters, audit status, and summary shape in domain `service.py`; put retryable external work in `tasks.py`.
- Log with `structlog`, not `print`.
- Include useful structured fields in logs, especially domain identifiers such as `provider_exchange_code`, `provider_instrument_code`, `bar_date`, and `provider`.
- Use one `@flow` per domain per schedule. Do not create mega-flows.

## Pipeline structure

Keep a strict boundary between reusable pipeline machinery and this app's
business vocabulary. Use the table below to decide where pipeline code belongs.
Paths are relative to `apps/pipelines/`. See
`apps/pipelines/docs/pipeline_architecture.md` for the full target picture and
domain migration pattern.

| Folder           | Question it answers                                                        | Belongs here                                                                                                                                                                                                                          |
| ---------------- | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `config/`        | What are the app's passive settings and names?                             | Environment-backed settings, stable enums, identity keys, and static non-secret defaults. Avoid executable wiring, registries, factories, or behavior.                                                                                |
| `control_plane/` | How is this app wired into Prefect, AWS, deployment, and runtime services? | App-specific Prefect block definitions, limit declarations, automation definitions, deployment defaults, AWS resource naming, and other runtime/control-plane wiring. Generic reusable helpers still belong in `core/`.               |
| `orchestration/` | How do pipeline jobs compose and run?                                      | Thin Prefect flow entrypoints, post-ingestion build policy, flow-check composition, dbt/build mappings, and app-level workflows that connect domains, dbt, and runtime behavior.                                                      |
| `core/`          | What reusable machinery exists without knowing this app?                   | Generic HTTP clients, storage/lake primitives, ingestion helpers, run tracking, schema/migration helpers, Prefect helper abstractions, dbt command execution, and small utilities.                                                    |
| `providers/`     | How do we talk to an external data provider?                               | Concrete provider clients, provider API models, provider identifier rules, and provider-owned parsing or normalization.                                                                                                               |
| `domains/`       | What business ingestion logic does this app own?                           | Domain models, Bronze table specs, datasets, parsers, tasks, request/result contracts, services, and domain-specific selection logic. New ingestion domains should follow the same local shape unless there is a clear reason not to. |
| `lakehouse/`     | Which concrete lake schemas, tables, and migrations does this app ship?    | App-level lake schema registries and migrations that compose core audit tables plus domain-owned Bronze tables. Generic lake clients and schema primitives stay in `core/lake/`.                                                      |
| `dbt/`           | How does Bronze become Silver and Gold?                                    | dbt sources, staging models, intermediate models, marts, seeds, macros, snapshots, and dbt tests. Python ingestion should hand off typed Bronze records; dbt owns transformation semantics.                                           |
| `dashboard/`     | How do operators inspect pipeline health?                                  | Streamlit operational dashboard code, read models, dashboard SQL, filters, tables, routing, and presentation code.                                                                                                                    |
| `scripts/`       | What command-line adapter does an operator run?                            | Thin entrypoints only: argument parsing, environment defaults, console output, and exit codes. Reusable behavior belongs outside `scripts/`.                                                                                          |

Boundary rules:

- `core/` must not import concrete providers, domains, app registries, app settings, Prefect block names, dbt asset groups, or business-specific table manifests.
- Provider packages may depend on `core`, but `core` must not depend on providers.
- Shared ingestion surfaces belong under `core/ingestion/`: landing targets for raw object storage and Bronze datasets for lake writes.
- S3 storage code belongs under `core/storage/s3/`.
- Lake access code belongs under `core/lake/`.
- Keep registries close to the thing they register: provider catalogs in `providers/`, dbt/build mappings in `orchestration/`, and runtime service wiring in `control_plane/`. Do not add domain registries until production code consumes them; domain identity lives in `config.domains`.
- Keep generic runtime infrastructure helpers such as Prefect block handles and health checks in `core.infrastructure`. Keep orchestration event vocabulary, event publishing, asset materialization helpers, automation sync, and global-limit sync in `core.orchestration`. `control_plane.prefect` declares this app's blocks, limits, automations, deployment defaults, and deployment sync behavior.
- Do not import `boto3` directly in domain code.
- Do not import `duckdb` directly in domain code.
- Do not read credentials from settings in tasks or flows. Load credentials from `BlockRegistry` at runtime.
- Use `get_settings()` as the only public settings accessor. Limit it to app composition modules such as `control_plane/`, `orchestration/`, and scripts, existing core client internals that still need lazy defaults, and tests that override settings between cases.
- Prefer function-local `get_settings()` calls; module-level calls are only for import-time declarations such as Prefect block definitions. Do not add new `core` settings imports; prefer explicit primitive values or app-owned factories.

## Data flow guardrails

- Always capture raw provider data with a landing target before parsing when S3 landing support is part of the flow.
- Parse provider or landed data into typed Bronze records in a separate step.
- Keep business logic out of ingestion tasks.
- Re-running a flow for the same logical partition should be idempotent.
- Bronze is the handoff from Python ingestion to dbt.

## dbt conventions

- Follow the detailed dbt design, naming, mart-shape, lineage, materialization, documentation, and testing conventions in `apps/pipelines/dbt/README.md#conventions`.
- Never put business logic in staging models. Staging is for type casting, renaming, normalization, and deduplication.
- Put analytics logic in marts, use `ref()` instead of hardcoded table names, and keep Bronze as the Python-to-dbt handoff.
- Keep raw lineage columns such as `source_uri` and `row_hash` out of Gold unless a consumer needs them directly.

## Tech stack reference

| Layer                 | Technology              |
| --------------------- | ----------------------- |
| Python runtime        | Python 3.14+            |
| Dependency management | uv                      |
| Orchestration         | Prefect                 |
| HTTP client           | httpx                   |
| Data validation       | Pydantic v2             |
| Data lake query       | DuckDB / MotherDuck     |
| Transformation        | dbt Core + dbt-duckdb   |
| Containerization      | Docker + Docker Compose |
| Studio backend        | TBD                     |
| Studio frontend       | TBD                     |
| Charts                | TBD                     |
| Monitoring            | TBD                     |
