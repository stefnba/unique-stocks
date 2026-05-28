WITH coverage AS (
    SELECT *
    FROM {{ ref('int_exchange_provider_coverage') }}
),

rolled_up AS (
    SELECT
        data_provider,
        provider_exchange_code,
        ANY_VALUE(exchange_catalog_name) AS exchange_catalog_name,
        ANY_VALUE(catalog_country_iso2) AS country_iso2,
        ANY_VALUE(currency) AS currency,
        MAX(catalog_snapshot_date) AS catalog_snapshot_date,
        COUNT(*) AS coverage_candidate_count,
        COUNT(mic) AS mapped_mic_count,
        SUM(CASE WHEN match_method = 'unmatched_mic_code' THEN 1 ELSE 0 END) AS unmatched_mic_count,
        SUM(CASE WHEN match_method = 'provider_bucket' THEN 1 ELSE 0 END) > 0 AS includes_provider_bucket
    FROM coverage
    GROUP BY 1, 2
)

SELECT
    data_provider,
    provider_exchange_code,
    exchange_catalog_name,
    country_iso2,
    currency,
    catalog_snapshot_date,
    coverage_candidate_count,
    mapped_mic_count,
    unmatched_mic_count,
    includes_provider_bucket,
    TRUE AS is_enabled_for_ingestion
FROM rolled_up
