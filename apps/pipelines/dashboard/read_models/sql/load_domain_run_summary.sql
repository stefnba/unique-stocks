SELECT
    domain,
    COUNT(*) AS runs,
    COUNT(*) FILTER (WHERE status = 'running') AS running_runs,
    COUNT(*) FILTER (WHERE status = 'completed') AS completed_runs,
    COUNT(*) FILTER (WHERE status IN ('failed', 'partial')) AS attention_runs,
    COALESCE(SUM(units_failed), 0) AS units_failed,
    COALESCE(SUM(rows_written), 0) AS rows_written,
    COALESCE(SUM(rows_rejected), 0) AS rows_rejected,
    MAX(started_at) AS latest_started_at
FROM pipeline.runs
WHERE {{ where_clauses }}
GROUP BY domain
ORDER BY attention_runs DESC, running_runs DESC, rows_rejected DESC, domain ASC
