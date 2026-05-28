# Pipeline Audit Tables

The `pipeline` lake schema is the data-plane audit trail for ingestion and transformation runs.
Prefect remains the orchestration control plane: it owns schedules, deployments, task states, retry
history, logs, work pools, and the UI. The lake audit tables own durable facts about the data:
what partitions were attempted, skipped, failed, landed, parsed, rejected, and written.

## Tables

| Table                       | Purpose                                                                                                                                |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `pipeline.runs`             | One row per logical pipeline invocation, including the Prefect flow run id when available.                                             |
| `pipeline.run_units`        | One row per domain work unit, such as an exchange/date, ticker backfill range, schedule code, or future fundamental period.            |
| `pipeline.landing_objects`  | Raw S3 landing objects produced or consumed by a run, linked to the run and optional work unit.                                        |
| `pipeline.rejections`       | Sampled structured parser rejection records. Counts on runs/units are exhaustive; row samples are capped to keep audit volume bounded. |
| `pipeline.dbt_invocations`  | One row per dbt command run by the dbt Prefect flow.                                                                                   |
| `pipeline.dbt_node_results` | Per-model/per-test results loaded from dbt `target/run_results.json`.                                                                  |

## Runs vs Run Units

`pipeline.runs` and `pipeline.run_units` have different grains:

| Table                | Grain                                   | Main question it answers                                                            |
| -------------------- | --------------------------------------- | ----------------------------------------------------------------------------------- |
| `pipeline.runs`      | One row per flow invocation             | Did this logical pipeline run finish, partially finish, fail, or skip?              |
| `pipeline.run_units` | One row per auditable unit inside a run | Which exact exchange, ticker, date, or schedule code succeeded, failed, or skipped? |

Use `pipeline.runs` for health dashboards and SLA checks. It stores the run-level parameters, Prefect flow run id,
overall status, start/end timestamps, aggregate counters, and compact summary/error fields.

Use `pipeline.run_units` for drill-down and replay/debugging. It stores the domain-specific unit key, unit status,
reason, source URI, per-unit row counters, and per-unit error details.

Example for one EOD backfill invocation:

```text
pipeline.runs
  run_id = 018f...
  flow_name = eod-price-backfill
  status = partial
  units_total = 5032
  units_failed = 3

pipeline.run_units
  run_id = 018f..., unit_type = exchange_backfill, unit_key = {"provider_exchange_code": "US", ...}
  run_id = 018f..., unit_type = ticker_backfill,   unit_key = {"ticker": "AAPL.US", ...}
  run_id = 018f..., unit_type = ticker_backfill,   unit_key = {"ticker": "MSFT.US", ...}
```

Every `run_units.run_id` should point at one `runs.run_id`. The current DuckDB/MotherDuck schema keeps that
relationship lightweight rather than enforcing foreign keys, so dbt tests or audit queries should check for orphan
units before promoting these tables into production monitoring.

## Statuses

Run statuses are intentionally data-oriented:

| Status      | Meaning                                                                                  |
| ----------- | ---------------------------------------------------------------------------------------- |
| `running`   | The run row has been created and work is in progress.                                    |
| `completed` | All planned work completed without unit failures or parser rejections.                   |
| `partial`   | The run completed, but one or more work units failed or parser rejections occurred.      |
| `failed`    | The flow aborted before reaching a clean terminal state.                                 |
| `skipped`   | There was no work to do, usually because every requested partition was already complete. |
| `cancelled` | Reserved for explicit cancellation handling.                                             |

Work-unit statuses are `completed`, `failed`, `skipped`, and `unsupported`. Reasons carry the domain detail,
for example `already_ingested`, `no_data`, `provider_404`, `no_new_rows`, or `symbol_failures`.

## Integration Pattern

Domain flows open one `pipeline.runs` row near the start of orchestration and close it after all expected work
units are processed. Tasks still do one concrete thing: fetch, land, parse, or write. The flow records audit facts
after each meaningful unit boundary.

Use `PipelineRunTracker` from `core.ingestion`:

```python
tracker = PipelineRunTracker()

with tracker.track_run(
    flow_name="instrument-refresh",
    domain="instrument",
    run_kind="snapshot",
    provider="eodhd",
    parameters={"snapshot_date": snapshot_date.isoformat()},
) as run:
    rows_raw = 0
    rows_written = 0
    with run.track_unit(
        unit_type="exchange_snapshot",
        unit_key={"provider_exchange_code": "US", "snapshot_date": snapshot_date.isoformat()},
    ) as unit:
        landing = await write_instrument_to_landing_zone(raw_rows, "US", snapshot_date)
        bronze = write_bronze_instrument(raw_rows, "US", snapshot_date, source_uri=landing.source_uri)
        unit.complete_with_landing(landing, reason=bronze.reason, rows_written=bronze.rows_written)
        rows_raw = landing.rows_raw or 0
        rows_written = bronze.rows_written

    run.complete(rows_raw=rows_raw, rows_written=rows_written)
```

Do not copy SQL into domain flows. Add new tracking behavior to `core.ingestion.run_tracking` and keep the
domain flow limited to choosing the right flow, unit type, unit key, dataset, counters, and reason. Avoid making
landing tasks write audit rows directly; Prefect tasks are retryable work steps and should return `LandingWrite`
metadata, while the flow or unit scope records the audit row with the active run/unit context.

`track_run()` marks the run failed on Python exceptions and raises if a flow exits without an explicit terminal state.
It currently treats interrupts such as Ctrl+C as failed; `cancelled` is reserved for a future explicit Prefect
cancellation hook. It cannot protect against worker process death, machine shutdown, or a lost database connection
after the initial `running` row. Monitor those with a scheduled query:

```sql
SELECT *
FROM pipeline.runs
WHERE status = 'running'
  AND started_at < now() - INTERVAL '2 hours'
ORDER BY started_at;
```

`pipeline.runs.units_total`, `units_succeeded`, `units_failed`, and `units_skipped` count actual rows written to
`pipeline.run_units`. Domain-specific planning metrics, such as exchange count or requested ticker count, belong in
`summary_json` so dashboards do not mix planned scope with recorded work units.

When a Bronze write is performed at a coarser grain than the work unit, keep `rows_written` at that coarser grain.
For example, EOD backfill writes ticker rows in batches, so ticker units store `rows_raw`, `rows_valid`, and
`rows_rejected`, while the exchange backfill rollup stores the batch-written `rows_written` total.

Parser rejection counts on `pipeline.runs` and `pipeline.run_units` are complete. `pipeline.rejections` intentionally
stores only a capped sample per unit, enough to debug the failure shape without turning the audit schema into a second
raw landing zone. Its `raw_hash` includes the entity key context so two tickers with identical malformed raw bars do
not collide in the same run.

## dbt

dbt runs are separate transformation flows, not hidden inside ingestion tasks. This keeps ingestion success,
dbt model failures, and dbt test failures independently visible.

The current deployment is `dbt-build/price-build`. It runs `dbt build` for the price staging and mart paths,
then reads `dbt/target/run_results.json` and writes:

- `pipeline.dbt_invocations`
- `pipeline.dbt_node_results`
- a top-level `pipeline.runs` row with `domain = 'dbt'`

The dbt run status follows node outcomes as well as the process return code: a non-zero process return code marks the
run `failed`, while a zero return code with failed node/test results marks the run `partial`.

dbt does not currently write `pipeline.run_units`. For `pipeline.runs` rows where `domain = 'dbt'`, `units_total`,
`units_succeeded`, and `units_failed` count dbt node results from `pipeline.dbt_node_results`, not rows from
`pipeline.run_units`. The node-result table is the dbt drill-down surface.

Use `parent_run_id` only when a dbt invocation is intentionally chained from a known ingestion run. Otherwise,
the dbt run remains independently auditable and can be joined by timestamp, selected model paths, or freshness
queries.

## When To Use Prefect Logs Instead

Do not write every log line to the lake. Use Prefect/structlog for narrative debugging and transient operational
messages. Use the lake audit tables for compact facts that need to survive with the data and support analytical
queries.
