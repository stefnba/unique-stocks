WITH catalog AS (
    SELECT *
    FROM {{ ref('stg_exchange_catalog') }}
),

catalog_mics AS (
    SELECT
        catalog.data_provider,
        catalog.provider_exchange_code,
        catalog.exchange_catalog_name,
        catalog.country,
        catalog.currency,
        catalog.country_iso2,
        catalog.country_iso3,
        NULLIF(TRIM(split_mics.operating_mic), '') AS catalog_operating_mic,
        catalog.snapshot_date AS catalog_snapshot_date,
        catalog.row_hash AS catalog_row_hash,
        catalog.source_uri AS catalog_source_uri,
        catalog.ingested_at AS catalog_ingested_at
    FROM catalog
    LEFT JOIN UNNEST(
        STRING_SPLIT(COALESCE(catalog.operating_mic_codes, ''), ',')
    ) AS split_mics (operating_mic) ON TRUE
),

mapped AS (
    SELECT
        catalog_mics.data_provider,
        catalog_mics.provider_exchange_code,
        catalog_mics.exchange_catalog_name,
        catalog_mics.catalog_operating_mic,
        universe.mic,
        universe.operating_mic,
        universe.mic_type,
        universe.exchange_name,
        universe.country_iso2 AS registry_country_iso2,
        catalog_mics.country_iso2 AS catalog_country_iso2,
        catalog_mics.currency,
        catalog_mics.catalog_snapshot_date,
        universe.registry_snapshot_date,
        catalog_mics.catalog_row_hash,
        universe.registry_row_hash,
        catalog_mics.catalog_source_uri,
        universe.registry_source_uri,
        catalog_mics.catalog_ingested_at,
        universe.registry_ingested_at,
        CASE
            WHEN universe.mic IS NOT NULL THEN 'operating_mic_code'
            WHEN catalog_mics.catalog_operating_mic IS NULL THEN 'provider_bucket'
            ELSE 'unmatched_mic_code'
        END AS match_method,
        CASE
            WHEN universe.mic IS NOT NULL THEN 'high'
            WHEN catalog_mics.catalog_operating_mic IS NULL THEN 'none'
            ELSE 'none'
        END AS match_confidence
    FROM catalog_mics
    LEFT JOIN {{ ref('int_exchange_universe') }} AS universe
        ON catalog_mics.catalog_operating_mic = universe.mic
)

SELECT
    data_provider,
    provider_exchange_code,
    exchange_catalog_name,
    catalog_operating_mic,
    mic,
    operating_mic,
    mic_type,
    exchange_name,
    registry_country_iso2,
    catalog_country_iso2,
    currency,
    match_method,
    match_confidence,
    mic IS NOT NULL AS is_mapped_to_exchange_universe,
    catalog_snapshot_date,
    registry_snapshot_date,
    catalog_row_hash,
    registry_row_hash,
    catalog_source_uri,
    registry_source_uri,
    catalog_ingested_at,
    registry_ingested_at
FROM mapped
