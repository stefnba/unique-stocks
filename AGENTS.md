# Agent guidelines

- Built to be lean. Add complexity only when you have a concrete reason.

## Coding Conventions

### Python

- Type hints on every function signature — no exceptions
- Pydantic v2 models for all external data, `extra="forbid"` to catch API drift early
- `async/await` throughout — httpx async client, async MotherDuck queries
- Keep provider fetches, parsing, and bronze writes in separate tasks.
- Domain flows should stay thin: schedule/date handling, idempotency, task orchestration, and run-state tracking.
- Log with structlog, not print() — every log line gets `ticker` and `bar_date` as structured fields
- One `@flow` per domain per schedule — no mega-flows
- S3 storage code belongs under `core/clients/storage/s3/`. Never import boto3 directly in domain code.

### dbt

- Never put business logic in staging models — staging is type-casting and renaming only
- Every model gets a `.yml` description file with column descriptions
- Test files mirror model structure exactly
- `ref()` over hardcoded table names always

### General

- No secrets in code or git — `.env` only
- Every new domain under `domains/` follows the same pattern: `models.py` + `flows.py` + `tasks.py` + `parsers.py`
- README in every app folder under `/apps` explaining how to run it standalone

### Git

- `main` — production-ready code only
- `archive/v1` — old codebase, preserved for reference, do not merge back
- Feature branches: `feat/eod-pipeline`, `feat/dbt-prices-mart`
- Commit style: `feat:`, `fix:`, `chore:`, `docs:` prefixes

## Tech Stack Reference

| Layer                 | Technology              | Version |
| --------------------- | ----------------------- | ------- |
| Python runtime        | Python                  | 3.14+   |
| Dependency management | uv                      | latest  |
| Orchestration         | Prefect                 | latest  |
| HTTP client           | httpx                   | latest  |
| Data validation       | Pydantic                | latest  |
| Data lake query       | DuckDB / MotherDuck     | latest  |
| Transformation        | dbt Core + dbt-duckdb   | latest  |
| Studio backend        | TBD                     | —       |
| Studio frontend       | TBD                     | —       |
| Charts                | TBD                     | -       |
| Containerisation      | Docker + Docker Compose | latest  |
| Monitoring            | TBD                     | -       |
