# Studio

`apps/studio` is reserved for the future Unique Stocks research UI.

## Status

Not implemented.

The studio stack is still an open architecture decision. Do not add dependencies, framework scaffolding, UI components, or deployment configuration here until that decision is made.

## Intended responsibility

The studio should become the read-only product surface for exploring Gold data: search, charts, watchlists, and investment research workflows.

## How to run

There is nothing to run yet. This app intentionally has no runtime, package manifest, environment file, or deployment target until the studio decision is made.

## Current guidance

- Keep this folder empty except for this README until the studio decision is made.
- Candidate stacks are still under consideration in `PLAN.md`.
- Design dbt Gold models so they can be queried by either a Python or TypeScript app later.
