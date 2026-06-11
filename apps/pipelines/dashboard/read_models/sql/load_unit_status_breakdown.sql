SELECT
    status,
    COUNT(*) AS units
FROM pipeline.run_units
WHERE run_id = {{ param() }}
GROUP BY status
ORDER BY units DESC, status ASC
