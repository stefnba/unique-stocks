SELECT
    rejection_id,
    unit_id,
    domain,
    entity_key_json,
    source_uri,
    reason,
    error_class,
    error_message,
    raw_sample_json,
    recorded_at
FROM pipeline.rejections
WHERE {{ where_clauses }}
ORDER BY recorded_at DESC
LIMIT {{ param() }}
