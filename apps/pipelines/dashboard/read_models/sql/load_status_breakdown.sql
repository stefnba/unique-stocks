SELECT
    status,
    COUNT(*) AS runs
FROM pipeline.runs
WHERE {{ where_clauses | default("TRUE") }}
GROUP BY status
ORDER BY runs DESC, status ASC
