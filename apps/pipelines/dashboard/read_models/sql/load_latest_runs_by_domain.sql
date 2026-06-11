WITH ranked AS (
    SELECT
        run_id,
        prefect_flow_run_id,
        flow_name,
        domain,
        run_kind,
        provider,
        status,
        started_at,
        completed_at,
        DATE_DIFF('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
        units_total,
        units_failed,
        rows_written,
        rows_rejected,
        error_class,
        error_message,
        ROW_NUMBER() OVER (PARTITION BY domain ORDER BY started_at DESC) AS row_number
    FROM pipeline.runs
    {{ where_sql | default("") }}
)

SELECT * EXCLUDE (row_number)
FROM ranked
WHERE row_number = 1
ORDER BY domain
