SELECT
    run_id,
    parent_run_id,
    prefect_flow_run_id,
    flow_name,
    domain,
    run_kind,
    provider,
    environment,
    code_version,
    parameters_json,
    target_window_start,
    target_window_end,
    status,
    started_at,
    completed_at,
    DATE_DIFF('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
    units_total,
    units_succeeded,
    units_failed,
    units_skipped,
    rows_raw,
    rows_valid,
    rows_rejected,
    rows_written,
    summary_json,
    error_class,
    error_message
FROM pipeline.runs
WHERE run_id = {{ param() }}
ORDER BY run_id ASC
LIMIT 1
