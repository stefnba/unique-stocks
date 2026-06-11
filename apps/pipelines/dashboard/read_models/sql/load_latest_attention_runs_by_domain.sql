WITH ranked AS (
    SELECT
        run_id,
        flow_name,
        domain,
        status,
        started_at,
        completed_at,
        units_failed,
        error_class,
        error_message,
        ROW_NUMBER() OVER (PARTITION BY domain ORDER BY started_at DESC) AS row_number
    FROM pipeline.runs
    WHERE {{ where_clauses | default("TRUE") }}
)

SELECT * EXCLUDE (row_number)
FROM ranked
WHERE row_number = 1
ORDER BY domain
