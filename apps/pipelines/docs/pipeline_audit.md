# Pipeline Audit Tables

The `pipeline` lake schema is the data-plane audit trail for ingestion and transformation runs.
Prefect remains the orchestration control plane: it owns schedules, deployments, task states, retry
history, logs, work pools, and the UI. The lake audit tables own durable facts about the data:
what partitions were attempted, skipped, failed, landed, parsed, rejected, and written.

## Tables

| Table                       | Purpose                                                                                                                                  |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `pipeline.runs`             | One row per logical pipeline invocation, including the Prefect flow run id when available.                                               |
| `pipeline.run_units`        | One row per domain work unit, such as an exchange/date, ticker backfill range, schedule code, or future fundamental period.              |
| `pipeline.landing_objects`  | Raw S3 landing objects produced or consumed by a run, linked to the run and optional work unit.                                          |
| `pipeline.rejections`       | Structured parser rejection records. The table exists now so parsers can start writing detailed rejects without a later schema redesign. |
| `pipeline.dbt_invocations`  | One row per dbt command run by the dbt Prefect flow.                                                                                     |
| `pipeline.dbt_node_results` | Per-model/per-test results loaded from dbt `target/run_results.json`.                                                                    |

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
run_id = tracker.start_run(
    flow_name="instrument-refresh",
    domain="instrument",
    run_kind="snapshot",
    provider="eodhd",
    parameters={"snapshot_date": snapshot_date.isoformat()},
)

unit_id = tracker.record_unit(
    run_id=run_id,
    domain="instrument",
    unit_type="exchange_snapshot",
    unit_key={"provider_exchange_code": "US", "snapshot_date": snapshot_date.isoformat()},
    status="completed",
    rows_raw=len(raw_rows),
    rows_written=written,
)
```

Do not copy SQL into domain flows. Add new tracking behavior to `core.ingestion.run_tracking` and keep the
domain flow limited to choosing the right domain, unit type, key, counters, and reason.

## dbt

dbt runs are separate transformation flows, not hidden inside ingestion tasks. This keeps ingestion success,
dbt model failures, and dbt test failures independently visible.

The current deployment is `dbt-build/price-build`. It runs `dbt build` for the price staging and mart paths,
then reads `dbt/target/run_results.json` and writes:

- `pipeline.dbt_invocations`
- `pipeline.dbt_node_results`
- a top-level `pipeline.runs` row with `domain = 'dbt'`

Use `parent_run_id` only when a dbt invocation is intentionally chained from a known ingestion run. Otherwise,
the dbt run remains independently auditable and can be joined by timestamp, selected model paths, or freshness
queries.

## When To Use Prefect Logs Instead

Do not write every log line to the lake. Use Prefect/structlog for narrative debugging and transient operational
messages. Use the lake audit tables for compact facts that need to survive with the data and support analytical
queries.
