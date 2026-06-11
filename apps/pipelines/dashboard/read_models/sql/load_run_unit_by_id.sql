SELECT
    unit_id,
    run_id,
    domain,
    provider,
    unit_type,
    unit_key_hash,
    unit_key_json,
    status,
    reason,
    source_uri,
    rows_raw,
    rows_valid,
    rows_rejected,
    rows_written,
    started_at,
    completed_at,
    DATE_DIFF('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
    error_class,
    error_message
FROM pipeline.run_units
WHERE run_id = {{ param() }} AND unit_id = {{ param() }}
ORDER BY unit_id ASC
LIMIT 1
