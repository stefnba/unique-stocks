WITH ranked AS (
    SELECT
        run_id,
        flow_name,
        domain,
        run_kind,
        status,
        completed_at,
        rows_written,
        rows_rejected,
        ROW_NUMBER() OVER (PARTITION BY domain ORDER BY completed_at DESC) AS row_number
    FROM pipeline.runs
    WHERE {{ where_clauses }}
)

SELECT * EXCLUDE (row_number)
FROM ranked
WHERE row_number = 1
ORDER BY domain
