# Agent Guidelines for Pipelines

## Python conventions

- Use type hints on every function signature.
- Use Pydantic v2 models for all external data.
- Set `extra="forbid"` on external API models to catch provider drift early.
- Use `async` / `await` throughout provider and orchestration paths.
- Use `httpx` async clients for HTTP.
- Keep provider fetches, landing writes, parsing, and Bronze writes in separate tasks.
- Keep domain flows thin: schedule handling, idempotency, task orchestration, and run-state tracking only.
- Log with `structlog`, not `print`.
- Include useful structured fields in logs, especially domain identifiers such as `provider_exchange_code`, `provider_instrument_code`, `bar_date`, and `provider`.
- Use one `@flow` per domain per schedule. Do not create mega-flows.

## Pipeline structure

- Keep a strict boundary between reusable pipeline infrastructure and this app's business vocabulary.
- Use these questions to decide where pipeline code belongs:
  - `apps/pipelines/config/` answers: "What are the app's passive settings and names?"
    Use it for environment-backed settings, stable enums, identity keys, and static non-secret defaults. Avoid executable wiring, registries, factories, or behavior.
  - `apps/pipelines/platform/` answers: "How is this app wired into Prefect, AWS, deployment, and runtime services?"
    Use it for app-specific Prefect block definitions, automation setup, concurrency limits, deployment registration, AWS resource naming, and other runtime/platform wiring. Generic reusable helpers still belong in `core/`.
  - `apps/pipelines/orchestration/` answers: "How do pipeline jobs compose and run?"
    Use it for flow composition, post-ingestion build policy, smoke-run composition, and app-level workflows that connect domains, dbt, and runtime behavior.
  - `apps/pipelines/core/` answers: "What reusable machinery exists without knowing this app?"
    Use it for generic HTTP clients, storage/lake primitives, ingestion helpers, run tracking, schema/migration helpers, Prefect helper abstractions, dbt command execution, and small utilities. `core/` must not import concrete providers, domains, app registries, app settings, Prefect block names, dbt asset groups, or business-specific table manifests.
  - `apps/pipelines/providers/` answers: "How do we talk to an external data provider?"
    Use it for concrete provider clients, provider API models, provider identifier rules, and provider-owned parsing or normalization. Providers may depend on `core`, but `core` must not depend on providers.
  - `apps/pipelines/domains/` answers: "What business ingestion logic does this app own?"
    Use it for domain models, Bronze table specs, datasets, parsers, tasks, flows, and domain-specific selection logic. New ingestion domains should follow the same local shape unless there is a clear reason not to.
  - `apps/pipelines/lakehouse/` answers: "Which concrete lake schemas, tables, and migrations does this app ship?"
    Use it for app-level lake schema registries and migrations that compose core audit tables plus domain-owned Bronze tables. Generic lake clients and schema primitives stay in `core/lake/`.
  - `apps/pipelines/dbt/` answers: "How does Bronze become Silver and Gold?"
    Use it for dbt sources, staging models, intermediate models, marts, seeds, macros, snapshots, and dbt tests. Python ingestion should hand off typed Bronze records; dbt owns transformation semantics.
  - `apps/pipelines/dashboard/` answers: "How do operators inspect pipeline health?"
    Use it for the Streamlit operational dashboard, read models, dashboard SQL, filters, tables, routing, and presentation code.
  - `apps/pipelines/scripts/` answers: "What command-line adapter does an operator run?"
    Use it for thin entrypoints only: argument parsing, environment defaults, console output, and exit codes. Reusable behavior belongs in `core/`, `platform/`, `orchestration/`, domains, providers, lakehouse modules, or dashboard modules.
- Shared ingestion surfaces belong under `apps/pipelines/core/ingestion/`: landing targets for raw object storage and Bronze datasets for lake writes.
- S3 storage code belongs under `apps/pipelines/core/storage/s3/`.
- Lake access code belongs under `apps/pipelines/core/lake/`.
- Do not import `boto3` directly in domain code.
- Do not import `duckdb` directly in domain code.
- Do not read credentials from `SETTINGS` in tasks or flows. Load credentials from `BlockRegistry` at runtime.
- Use `SETTINGS` only when constructing initial app-specific Prefect block instances at registry definition time.
- Use `get_settings()` only in app composition modules such as `platform/`, `orchestration/`, and scripts, in existing core client internals that still need lazy defaults, and in tests that need to override settings between cases. Do not add new `core` settings imports; prefer explicit primitive values or app-owned factories.

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
