SELECT
    unit.unit_id,
    unit.run_id,
    {{ flow_sql | default("CAST(NULL AS VARCHAR)") }} AS flow_name,
    {{ run_kind_sql | default("CAST(NULL AS VARCHAR)") }} AS run_kind,
    {{ domain_sql | default("unit.domain") }} AS domain,
    {{ provider_sql | default("unit.provider") }} AS provider,
    unit.unit_type,
    unit.unit_key_hash,
    unit.unit_key_json,
    unit.status,
    unit.reason,
    unit.source_uri,
    unit.rows_raw,
    unit.rows_valid,
    unit.rows_rejected,
    unit.rows_written,
    unit.started_at,
    unit.completed_at,
    DATE_DIFF('second', unit.started_at, COALESCE(unit.completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
    unit.error_class,
    unit.error_message
FROM pipeline.run_units AS unit
{{ join_sql | default("") }}
WHERE {{ where_clauses | default("TRUE") }}
ORDER BY
    CASE unit.status
        WHEN 'failed' THEN 1
        WHEN 'unsupported' THEN 2
        WHEN 'skipped' THEN 3
        WHEN 'running' THEN 4
        ELSE 5
    END ASC,
    unit.completed_at DESC NULLS LAST,
    unit.started_at DESC NULLS LAST
LIMIT {{ param() }}
