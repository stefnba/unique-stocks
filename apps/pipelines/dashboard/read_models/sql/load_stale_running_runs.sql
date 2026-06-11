SELECT
    run_id,
    prefect_flow_run_id,
    flow_name,
    domain,
    run_kind,
    provider,
    status,
    started_at,
    completed_at,
    DATE_DIFF('second', started_at, CURRENT_TIMESTAMP) AS running_seconds,
    DATE_DIFF('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
    units_total,
    units_failed,
    rows_written,
    rows_rejected,
    error_class,
    error_message
FROM pipeline.runs
WHERE {{ where_clauses | default("TRUE") }}
ORDER BY started_at
LIMIT {{ param() }}
