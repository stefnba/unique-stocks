SELECT
    landing.landing_id,
    landing.run_id,
    landing.unit_id,
    {{ run_flow_sql }} AS flow_name,
    {{ domain_sql }} AS domain,
    {{ provider_sql }} AS provider,
    landing.dataset,
    landing.source_uri,
    landing.partition_json,
    landing.rows_raw,
    landing.byte_count,
    landing.content_hash,
    landing.recorded_at
FROM pipeline.landing_objects AS landing
{{ run_join_sql }}
{{ unit_join_sql }}
WHERE {{ where_clauses }}
ORDER BY landing.recorded_at DESC
LIMIT {{ param() }}
