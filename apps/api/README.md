# API

`apps/api` is reserved for a future API surface for Unique Stocks.

## Status

Not implemented.

The API boundary is still an open architecture decision. Do not add dependencies, framework scaffolding, routes, or deployment configuration here until that decision is made.

## Intended responsibility

If v1 needs an API, this app should provide a thin read layer over Gold data for app surfaces such as the studio. It should not own ingestion, transformation, or business logic that belongs in pipelines or dbt.

## How to run

There is nothing to run yet. This app intentionally has no runtime, package manifest, environment file, or deployment target until the API decision is made.

## Current guidance

- Keep this folder empty except for this README until the API decision is made.
- Do not duplicate pipeline or dbt behavior here.
- Do not add secrets or environment examples before a concrete runtime exists.
