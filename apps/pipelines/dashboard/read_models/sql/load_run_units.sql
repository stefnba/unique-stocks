SELECT
    unit_id,
    unit_type,
    unit_key_hash,
    unit_key_json,
    status,
    reason,
    provider,
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
WHERE run_id = {{ param() }}
ORDER BY
    CASE status
        WHEN 'failed' THEN 1
        WHEN 'unsupported' THEN 2
        WHEN 'skipped' THEN 3
        ELSE 4
    END,
    completed_at DESC NULLS LAST,
    started_at DESC NULLS LAST
LIMIT {{ param() }}
