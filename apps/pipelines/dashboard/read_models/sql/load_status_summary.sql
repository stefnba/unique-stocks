SELECT
    COUNT(*) AS total_runs,
    COUNT(*) FILTER (WHERE status = 'running') AS running_runs,
    COUNT(*) FILTER (WHERE status = 'completed') AS completed_runs,
    COUNT(*) FILTER (WHERE status = 'partial') AS partial_runs,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
    COUNT(*) FILTER (WHERE status IN ('failed', 'partial')) AS attention_runs,
    COALESCE(SUM(units_failed), 0) AS units_failed,
    COALESCE(SUM(rows_written), 0) AS rows_written,
    COALESCE(SUM(rows_rejected), 0) AS rows_rejected
FROM pipeline.runs
WHERE {{ where_clauses | default("TRUE") }}
