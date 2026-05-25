# Agent Guidelines

This file is for AI and coding agents working in this repository. Human onboarding and run commands belong in `README.md` files. Product and architecture planning belongs in `PLAN.md`.

## Operating principles

- Keep the project lean. Add complexity only when there is a concrete reason.
- Prefer existing patterns in the repo over new abstractions.
- Keep edits scoped to the requested behavior.
- Do not commit secrets. Local secrets belong in `.env` files only.
- Preserve unrelated user changes in the worktree.

## Python conventions

- Use type hints on every function signature.
- Use Pydantic v2 models for all external data.
- Set `extra="forbid"` on external API models to catch provider drift early.
- Use `async` / `await` throughout provider and orchestration paths.
- Use `httpx` async clients for HTTP.
- Keep provider fetches, landing writes, parsing, and Bronze writes in separate tasks.
- Keep domain flows thin: schedule handling, idempotency, task orchestration, and run-state tracking only.
- Log with `structlog`, not `print`.
- Include useful structured fields in logs, especially domain identifiers such as `ticker`, `bar_date`, `exchange`, and `provider`.
- Use one `@flow` per domain per schedule. Do not create mega-flows.

## Pipeline structure

- Every new ingestion domain under `apps/pipelines/domains/` follows the same pattern: `models.py`, `tables.py`, `datasets.py`, `parsers.py`, `tasks.py`, and `flows.py`.
- Domain models live with the domain that owns them.
- Shared infrastructure belongs under `apps/pipelines/core/`.
- Shared ingestion surfaces belong under `apps/pipelines/core/ingestion/`: landing targets for raw object storage and Bronze datasets for lake writes.
- App-level configuration belongs under `apps/pipelines/config/`: `settings.py` for environment-variable-backed settings, `blocks.py` for the Prefect block registry.
- Provider-specific clients and raw provider models belong under `apps/pipelines/providers/`.
- S3 storage code belongs under `apps/pipelines/core/clients/storage/s3/`.
- Lake access code belongs under `apps/pipelines/core/clients/lake/` and the compatibility wrapper in `apps/pipelines/core/lake.py`.
- Do not import `boto3` directly in domain code.
- Do not import `duckdb` directly in domain code.
- Do not read credentials from `SETTINGS` in tasks or flows. Load credentials from `BlockRegistry` at runtime.
- Use `SETTINGS` only in `config/blocks.py` to construct the initial block instances at registry definition time.
- Use `get_settings()` only inside core client internals (`_settings()` methods) where a lazy import is needed to avoid circular imports, and in tests that need to override settings between cases.

## Data flow guardrails

- Always capture raw provider data with a landing target before parsing when S3 landing support is part of the flow.
- Parse provider or landed data into typed Bronze records in a separate step.
- Keep business logic out of ingestion tasks.
- Re-running a flow for the same logical partition should be idempotent.
- Bronze is the handoff from Python ingestion to dbt.

## dbt conventions

- Never put business logic in staging models. Staging is for type casting, renaming, normalization, and deduplication.
- Put analytics logic in marts.
- Use `ref()` instead of hardcoded table names.
- Every model gets a `.yml` description file with column descriptions.
- Test files mirror model structure.
- Add tests for primary keys, important not-null columns, accepted enum values, and domain-specific sanity checks.

## App boundaries

- Every folder under `apps/` must have a README explaining its status and how to run it standalone.
- `apps/pipelines` is the active Python ingestion app.
- `apps/api` is reserved for a future API surface. Do not add dependencies or implementation until the API boundary is decided.
- `apps/studio` is reserved for the future research UI. Do not add dependencies or implementation until the studio stack is decided.
- Keep root-level setup and app runbooks out of `AGENTS.md`.

## Git workflow

- `main` is production-ready only.
- `archive/v1` preserves the old codebase and should not be merged back.
- Use feature branches such as `feat/eod-pipeline` or `feat/dbt-prices-mart`.
- Use commit prefixes: `feat:`, `fix:`, `chore:`, and `docs:`.

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
