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

Routes use query params for bookmarkable drill-down:

- `?page=overview` — triage queue, domain health, collapsed activity/lookup
- `?page=run&run_id=<run_id>` — run investigation with unit master-detail
- `?page=unit&run_id=<run_id>&unit_id=<unit_id>` — unit evidence view

Select a row in any run table to open run detail. On the run page, select a unit row
to inspect landing/rejection evidence inline, or open the full unit page.

## Layout

`app.py` is the Streamlit entrypoint. Page rendering lives under `views/`, shared
formatting under `formatting.py`, table helpers under `tables.py`, cached loaders
under `loaders.py`, and read-only SQL under `queries.py`.

## Local Run

From `apps/pipelines`:

```bash
make dashboard
```

Set `MOTHERDUCK_TOKEN` to inspect the production lake. Without it, the dashboard reads
`LOCAL_LAKE_PATH`.

## Production

Production should run the dashboard as its own service:

```text
pipelines-worker       Prefect worker, ingestion, dbt, migrations
pipelines-dashboard    Streamlit dashboard, read-only lake queries
```

Prefer `DASHBOARD_MOTHERDUCK_TOKEN` for a read-only MotherDuck token. If it is not set,
the dashboard falls back to `MOTHERDUCK_TOKEN`.
