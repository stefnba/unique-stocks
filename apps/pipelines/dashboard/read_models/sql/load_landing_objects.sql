SELECT
    landing_id,
    run_id,
    unit_id,
    domain,
    dataset,
    provider,
    source_uri,
    partition_json,
    rows_raw,
    byte_count,
    content_hash,
    recorded_at
FROM pipeline.landing_objects
WHERE {{ where_clauses }}
ORDER BY recorded_at DESC
LIMIT {{ param() }}
