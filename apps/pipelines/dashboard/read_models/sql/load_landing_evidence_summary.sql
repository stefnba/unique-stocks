SELECT
    COUNT(*) AS landing_objects,
    COALESCE(SUM(byte_count), 0) AS landing_bytes
FROM pipeline.landing_objects
WHERE {{ where_clauses | default("TRUE") }}
