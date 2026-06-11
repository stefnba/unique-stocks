SELECT
    landing.landing_id,
    landing.run_id,
    landing.unit_id,
    {{ run_flow_sql | default("CAST(NULL AS VARCHAR)") }} AS flow_name,
    {{ domain_sql | default("landing.domain") }} AS domain,
    {{ provider_sql | default("landing.provider") }} AS provider,
    landing.dataset,
    landing.source_uri,
    landing.partition_json,
    landing.rows_raw,
    landing.byte_count,
    landing.content_hash,
    landing.recorded_at
FROM pipeline.landing_objects AS landing
{{ run_join_sql | default("") }}
{{ unit_join_sql | default("") }}
WHERE {{ where_clauses | default("TRUE") }}
ORDER BY landing.recorded_at DESC
LIMIT {{ param() }}
