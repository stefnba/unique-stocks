SELECT
    CAST(started_at AS DATE) AS run_date,
    domain,
    COUNT(*) AS runs,
    COUNT(*) FILTER (WHERE status IN ('failed', 'partial')) AS attention_runs,
    COALESCE(SUM(rows_written), 0) AS rows_written,
    COALESCE(SUM(rows_rejected), 0) AS rows_rejected
FROM pipeline.runs
WHERE {{ where_clauses | default("TRUE") }}
GROUP BY run_date, domain
ORDER BY run_date, domain
