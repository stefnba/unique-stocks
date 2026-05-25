# Unique Stocks Plan

This document is the internal product and architecture plan for Unique Stocks. It explains what v1 is meant to become, which decisions are fixed for now, what is still open, and the order of work.

It is not a setup guide or code reference. Runbooks live in README files. Agent-specific coding rules live in `AGENTS.md`.

## Product goal

Build a self-hosted financial data platform that can ingest market data, transform it into trustworthy analytics tables, and expose it through a simple research surface.

The product should stay lean:

- Low operational burden.
- Cheap enough to run on a small VPS.
- Replayable data ingestion.
- Clear separation between raw data, typed ingestion, transformations, and user-facing queries.
- Good enough architecture for v1 without adding large-scale systems early.

## V1 scope

V1 focuses on end-of-day equity market data and the data foundations needed for a future studio.

Included:

- EODHD as the first market data provider.
- End-of-day OHLCV prices.
- Exchange and instrument reference data.
- Fundamentals ingestion after the prices path is stable.
- Prefect flows for scheduled and manual ingestion.
- S3 landing storage for provider-validated raw payloads.
- DuckDB / MotherDuck as the primary analytical store.
- dbt Core for Silver and Gold transformations.
- A future read-only studio for search, charts, and watchlists.

Excluded for v1:

- Intraday or real-time data.
- Options chains.
- Crypto and FX price/history ingestion.
- Multi-tenant SaaS behavior.
- User authentication.
- Alerts and notifications.
- Apache Iceberg.
- Trino.
- Complex distributed orchestration.

## Architecture decisions

These are the default decisions for v1.

| Decision                                        | Rationale                                                                                                                                                                                                                                                                                                                        |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MotherDuck over S3 + Trino                      | Lower operations, enough performance for the expected v1 scale, easy local DuckDB fallback.                                                                                                                                                                                                                                      |
| S3 landing before parsing                       | Provider-validated raw payloads stay replayable and auditable while strict provider models catch API drift before storage.                                                                                                                                                                                                       |
| Medallion lake model                            | Keeps raw capture, typed ingestion, cleanup, and business logic in separate layers.                                                                                                                                                                                                                                              |
| Prefect over Airflow                            | Python-native orchestration with less infrastructure.                                                                                                                                                                                                                                                                            |
| dbt Core for transformations                    | SQL transformations stay explicit, testable, and separate from ingestion code.                                                                                                                                                                                                                                                   |
| Plain Parquet archive over Iceberg              | v1 data is mostly append-oriented; Iceberg adds catalog and table-format complexity too early.                                                                                                                                                                                                                                   |
| EODHD first                                     | One provider covers the first required domains and keeps integration complexity low.                                                                                                                                                                                                                                             |
| Prefect blocks as the runtime credential source | Tasks and flows load credentials from named Prefect blocks at runtime. This decouples secret values from the deployed codebase and allows updates in the Prefect UI without a redeploy. Environment variables seed the block registry at startup; the block registry is the single credential boundary that domain code crosses. |
| Bronze reference data as daily snapshots        | Reference domains (exchanges, instruments) store one row per record per ingestion day, even when the underlying data has not changed. This makes Bronze a faithful audit log of what each pipeline run produced. Deduplication and change tracking (SCD2 or latest-only) are Silver responsibilities handled in dbt.             |

## System shape

The platform is organized around deployable apps and standalone data tooling:

- `apps/pipelines` is the active Python ingestion app.
- `apps/pipelines/dbt` owns transformation models from Bronze to Silver to Gold.
- `infra` owns local infrastructure and database initialization.
- `apps/api` is reserved for a future API surface.
- `apps/studio` is reserved for the future research UI.

The intended data flow is:

1. Provider data is fetched by a Prefect task.
2. The provider response is validated against provider models and stored in S3 landing storage through a landing target.
3. A separate parsing step normalizes provider or landed data into typed Bronze parse results.
4. A Bronze dataset writes normalized rows to the lake with provider lineage and source URI metadata.
5. dbt transforms Bronze into Silver and Gold.
6. Future app surfaces query Gold data.

Ingestion code should stay thin. Provider fetches, landing writes, parsing, and Bronze writes are separate responsibilities. Domain flows coordinate schedules, idempotency, run state, and task orchestration.

## Data layer contract

S3 landing storage is the canonical replay source for provider-validated raw external data. Nothing in landing should depend on downstream schema choices.

MotherDuck stores the lake schemas:

| Layer             | Owner            | Purpose                                                                 |
| ----------------- | ---------------- | ----------------------------------------------------------------------- |
| Bronze            | Python pipelines | Typed, validated, append-oriented records with provider lineage.        |
| Silver            | dbt              | Deduplicated and normalized tables with no business logic.              |
| Gold              | dbt              | Analytics-ready models for search, charts, indicators, and app queries. |
| Pipeline metadata | Python pipelines | Run state, idempotency support, and operational metadata.               |

Bronze is the handoff point between Python ingestion and dbt. Python should not implement analytics logic that belongs in Silver or Gold. dbt staging models should not contain business logic.

## Domain roadmap

| Domain        | Data                                              | Schedule                    | Status                                  |
| ------------- | ------------------------------------------------- | --------------------------- | --------------------------------------- |
| `eod_price`   | Daily OHLCV bars                                  | Weekdays after market close | Active first path.                      |
| `exchange`    | Exchange reference list                           | Manual or monthly           | Landing + Bronze ingestion complete.    |
| `instrument`  | Tradable instruments per exchange and asset class | Weekly                      | Landing + Bronze ingestion implemented. |
| `fundamental` | Financial statements, ratios, dividends, splits   | Quarterly or manual         | Stubbed / planned.                      |

Implementation order:

1. Stabilize EOD prices ingestion end to end.
2. Add S3 landing writes before Bronze writes.
3. Make idempotency and run tracking reliable.
4. Add exchange and instrument reference ingestion.
5. Build the first dbt Silver and Gold prices models.
6. Add fundamentals ingestion.
7. Decide the API and studio stack.
8. Build the first read-only research workflows.

## Milestones

### Milestone 1: Reliable prices ingestion

- Daily and manual backfill deployments work for EOD prices.
- Re-running a date does not create duplicates.
- Provider models reject unexpected API drift.
- Pipeline failures are visible through run state and logs.

### Milestone 2: Replayable landing zone

- Raw provider responses are written before parsing.
- Bronze records retain lineage to their raw source.
- Reprocessing Bronze from landing data does not require a new provider fetch.

### Milestone 3: dbt foundation

- Bronze to Silver prices transformation exists.
- Gold daily prices model exists for app queries.
- dbt model descriptions and tests are present.
- Staging remains limited to casting, naming, normalization, and deduplication.

### Milestone 4: Reference data

- Exchanges and instruments flows are implemented.
- Reference data supports current-instrument lookup.
- Gold models can support search and symbol discovery.

### Milestone 5: Studio decision

- Decide whether `apps/studio` is Streamlit or a TypeScript web app.
- Decide whether `apps/api` is needed for v1 or can wait.
- Define the minimum read-only workflows for the first usable studio.

## Open decisions

| Decision                    | Options                                                            | Default until decided           |
| --------------------------- | ------------------------------------------------------------------ | ------------------------------- |
| Studio stack                | Streamlit, or TypeScript frontend with a lightweight API           | Do not add studio dependencies. |
| API boundary                | No API for v1, Hono/TypeScript API, or Python API                  | Keep `apps/api` empty.          |
| Charting library            | TradingView Lightweight Charts, Plotly, or native Streamlit charts | Wait for studio stack decision. |
| Deployment shape for studio | Same VPS, separate service, or static frontend plus API            | Wait for stack decision.        |
| Provider expansion          | Add more EODHD domains first, or introduce a second provider       | Finish EODHD v1 path first.     |

## Documentation boundaries

- Root `README.md` is the human entrypoint and command index.
- App READMEs are standalone runbooks for each deployable app.
- `AGENTS.md` is only for coding-agent behavior and guardrails.
- This plan should stay strategic and implementation-facing, without code examples or operational command sequences.
