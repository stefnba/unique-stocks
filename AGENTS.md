# Agent Guidelines

This file is for AI and coding agents working in this repository. Human onboarding and run commands belong in `README.md` files. Product and architecture planning belongs in `PLAN.md`.

## Operating principles

- Keep the project lean. Add complexity only when there is a concrete reason.
- Prefer existing patterns in the repo over new abstractions.
- Keep edits scoped to the requested behavior.
- Do not commit secrets. Local secrets belong in `.env` files only.
- Preserve unrelated user changes in the worktree.

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
