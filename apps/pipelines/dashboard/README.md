# Pipeline Dashboard

`dashboard/` is the Streamlit operations surface for the `apps/pipelines` audit trail.
It stays inside the pipelines app so it can reuse the existing settings and lake client,
but it runs as a separate process/container from the Prefect worker.

## Data Source

The dashboard reads the lake `pipeline` schema through `DataLakeClient(read_only=True)`.
It treats `pipeline.runs` as the summary table and loads drill-down data from
`pipeline.run_units`, `pipeline.landing_objects`, `pipeline.rejections`,
`pipeline.dbt_invocations`, and `pipeline.dbt_node_results`.

## Pages

The dashboard uses Streamlit's native page navigation. Overview routes are visible in
the top navigation; detail routes are hidden but registered for direct links:

- `/` — action queue, at-risk domain preview, movement and evidence (collapsed)
- `/domains` — domain health matrix, movement and evidence, latest attention runs
- `/domain-detail?domain=<domain>` — one-domain freshness, movement, evidence, and attention runs
- `/runs` — searchable run browser (`?domain=` and `?status=` preset filters)
- `/run-detail?run_id=<run_id>` — run investigation with unit master-detail
- `/run-units` — searchable work-unit browser (`?domain=`, `?status=`, and `?run_id=` preset filters)
- `/run-unit-detail?run_id=<run_id>&unit_id=<unit_id>` — unit evidence view
- `/landing-objects` — searchable landing-object browser (`?domain=`, `?run_id=`, and `?unit_id=` preset filters)
- `/landing-object-detail?landing_id=<landing_id>` — raw landing-object metadata and parent context

Select a linked ID in any table to open the corresponding detail page. On the run page,
select a unit row to inspect landing/rejection evidence inline, or open the full unit page.

## Layout

`app.py` is intentionally thin. It configures Streamlit, registers navigation, renders
shared chrome, and runs the selected page.

- `bootstrap.py` — page config, dashboard token preference, shared refresh action
- `navigation.py` — visible and hidden `st.Page` registrations
- `style.py` — small CSS overrides for Streamlit primitives
- `routing.py` — URL builders and query-param reads
- `views/` — page-level rendering only
- `filters.py` — shared Streamlit controls and normalized filter payloads
- `domain_health.py`, `charts.py`, and `tables.py` — reusable presentation helpers
- `loaders.py` — cached lake loaders used by views
- `read_models/queries.py` — read-only lake query functions
- `read_models/sql/` — file-backed SQL templates used by the dashboard query functions

Keep new pages thin: add page rendering under `views/`, cached data assembly in
`loaders.py`, and lake reads under `read_models/`. Keep direct `duckdb` and settings
access out of views.

## Setup

The dashboard is installed from the pipelines app with the optional `dashboard`
dependency group. From `apps/pipelines`:

```bash
uv sync --extra dashboard
```

For local lake reads, set `LAKE_NAME` and `LOCAL_LAKE_PATH` the same way the pipelines
app does. For MotherDuck reads, prefer `DASHBOARD_MOTHERDUCK_TOKEN`; the app copies it
to `MOTHERDUCK_TOKEN` at startup so the shared lake client can connect with a read-only
token. If `DASHBOARD_MOTHERDUCK_TOKEN` is not set, the dashboard falls back to the
regular `MOTHERDUCK_TOKEN`.

## Local Run

From `apps/pipelines`:

```bash
make dashboard
```

Use `STREAMLIT_SERVER_PORT=<port> make dashboard` when the default port is already in
use.

## Production

Production should run the dashboard as its own service:

```text
pipelines-worker       Prefect worker, ingestion, dbt, migrations
pipelines-dashboard    Streamlit dashboard, read-only lake queries
```

Configure the production service with `DASHBOARD_MOTHERDUCK_TOKEN` instead of the
worker's broader write-capable token.
