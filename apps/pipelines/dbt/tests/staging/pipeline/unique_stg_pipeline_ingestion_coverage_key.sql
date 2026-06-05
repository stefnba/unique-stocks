SELECT
    domain,
    data_provider,
    unit_type,
    unit_key_hash,
    status
FROM {{ ref('stg_pipeline_ingestion_coverage') }}
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(*) > 1
