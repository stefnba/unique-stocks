SELECT
    status,
    COUNT(*) AS runs
FROM pipeline.runs
WHERE {{ where_clauses }}
GROUP BY status
ORDER BY runs DESC, status ASC
