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
- `apps/pipelines/core/` is the generic foundation. It may contain reusable clients, storage/lake primitives, ingestion helpers, orchestration utilities, audit/run-tracking helpers, and small dependency-free utility functions. It must not know concrete providers, concrete domains, app registries, Prefect block names, dbt asset groups, app settings, or business-specific table manifests.
- `apps/pipelines/config/` is app-specific configuration only. Use it for environment-variable-backed settings, non-secret constants, app vocabulary enums, and Prefect block definitions. Avoid putting composition functions, registries, lookup helpers, or behavior in `config/`; those belong in the app layer that uses the configuration.
- `apps/pipelines/providers/` contains app-specific provider implementations: concrete HTTP clients, provider API models, provider parsers when they are provider-owned, and provider-specific constants. Provider packages may depend on generic `core` interfaces, but `core` must not import providers.
- `apps/pipelines/domains/` contains app-specific domain implementations: domain models, tables, datasets, parsers, tasks, and flows. Every new ingestion domain follows the same pattern: `models.py`, `tables.py`, `datasets.py`, `parsers.py`, `tasks.py`, and `flows.py`. Domain models live with the domain that owns them.
- App-specific wiring, registries, and manifests should be explicit app-layer modules outside `core` and outside pure `config`. Prefer placing them with their owner: provider registration in `providers/`, domain registration in `domains/`, and deployment/infrastructure wiring in `infrastructure/`. They may compose `config`, `domains`, `providers`, and generic `core` surfaces, but they must not make `core` depend on app-specific concepts.
- Shared ingestion surfaces belong under `apps/pipelines/core/ingestion/`: landing targets for raw object storage and Bronze datasets for lake writes.
- S3 storage code belongs under `apps/pipelines/core/storage/s3/`.
- Lake access code belongs under `apps/pipelines/core/lake/`.
- Do not import `boto3` directly in domain code.
- Do not import `duckdb` directly in domain code.
- Do not read credentials from `SETTINGS` in tasks or flows. Load credentials from `BlockRegistry` at runtime.
- Use `SETTINGS` only in `config/blocks.py` to construct the initial block instances at registry definition time.
- Use `get_settings()` only in app composition modules such as `orchestration/` and scripts, in existing core client internals that still need lazy defaults, and in tests that need to override settings between cases. Do not add new `core` settings imports; prefer explicit primitive values or app-owned factories.

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
