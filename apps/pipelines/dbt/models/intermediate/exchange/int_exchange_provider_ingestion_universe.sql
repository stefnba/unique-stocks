WITH coverage AS (
    SELECT *
    FROM {{ ref('int_exchange_provider_coverage') }}
),

curated_provider_namespaces AS (
    SELECT
        LOWER(TRIM(CAST(data_provider AS VARCHAR))) AS data_provider,
        UPPER(TRIM(CAST(provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        LOWER(TRIM(CAST(provider_code_kind AS VARCHAR))) AS provider_code_kind,
        NULLIF(TRIM(CAST(display_name AS VARCHAR)), '') AS exchange_catalog_name,
        NULLIF(UPPER(TRIM(CAST(asset_type_hint AS VARCHAR))), '') AS asset_type_hint,
        NULLIF(UPPER(TRIM(CAST(country_iso2 AS VARCHAR))), '') AS country_iso2,
        NULLIF(UPPER(TRIM(CAST(currency AS VARCHAR))), '') AS currency,
        CAST(is_enabled_for_instrument AS BOOLEAN) AS is_enabled_for_instrument,
        CAST(is_enabled_for_eod_price AS BOOLEAN) AS is_enabled_for_eod_price,
        NULLIF(UPPER(TRIM(CAST(verification_symbol AS VARCHAR))), '') AS verification_symbol,
        CAST(last_verified_on AS DATE) AS last_verified_on,
        NULLIF(TRIM(CAST(notes AS VARCHAR)), '') AS notes
    FROM {{ ref('eodhd_provider_namespaces') }}
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
),

provider_catalog_codes AS (
    SELECT
        data_provider,
        provider_exchange_code,
        CASE
            WHEN mapped_mic_count > 0 THEN 'exchange'
            ELSE 'provider_bucket'
        END AS provider_code_kind,
        'provider_catalog' AS source_kind,
        exchange_catalog_name,
        CAST(NULL AS VARCHAR) AS asset_type_hint,
        country_iso2,
        currency,
        catalog_snapshot_date,
        COUNT(*) OVER () AS catalog_code_count,
        coverage_candidate_count,
        mapped_mic_count,
        unmatched_mic_count,
        includes_provider_bucket,
        data_provider = 'eodhd' AS is_enabled_for_instrument,
        data_provider = 'eodhd' AS is_enabled_for_eod_price,
        data_provider = 'eodhd' AND mapped_mic_count > 0 AS is_enabled_for_fundamental,
        data_provider = 'eodhd' AS is_enabled_for_ingestion,
        CAST(NULL AS VARCHAR) AS verification_symbol,
        CAST(NULL AS DATE) AS last_verified_on,
        CAST(NULL AS VARCHAR) AS notes
    FROM rolled_up
),

curated_namespace_codes AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_code_kind,
        'curated_seed' AS source_kind,
        exchange_catalog_name,
        asset_type_hint,
        country_iso2,
        currency,
        CAST(NULL AS DATE) AS catalog_snapshot_date,
        CAST(NULL AS INTEGER) AS catalog_code_count,
        0 AS coverage_candidate_count,
        0 AS mapped_mic_count,
        0 AS unmatched_mic_count,
        FALSE AS includes_provider_bucket,
        is_enabled_for_instrument,
        is_enabled_for_eod_price,
        provider_code_kind = 'index_namespace' AS is_enabled_for_fundamental,
        is_enabled_for_instrument
        OR is_enabled_for_eod_price
        OR provider_code_kind = 'index_namespace'
            AS is_enabled_for_ingestion,
        verification_symbol,
        last_verified_on,
        notes
    FROM curated_provider_namespaces
)

SELECT
    data_provider,
    provider_exchange_code,
    provider_code_kind,
    source_kind,
    exchange_catalog_name,
    asset_type_hint,
    country_iso2,
    currency,
    catalog_snapshot_date,
    catalog_code_count,
    coverage_candidate_count,
    mapped_mic_count,
    unmatched_mic_count,
    includes_provider_bucket,
    is_enabled_for_instrument,
    is_enabled_for_eod_price,
    is_enabled_for_fundamental,
    is_enabled_for_ingestion,
    verification_symbol,
    last_verified_on,
    notes
FROM provider_catalog_codes

UNION ALL

SELECT
    data_provider,
    provider_exchange_code,
    provider_code_kind,
    source_kind,
    exchange_catalog_name,
    asset_type_hint,
    country_iso2,
    currency,
    catalog_snapshot_date,
    catalog_code_count,
    coverage_candidate_count,
    mapped_mic_count,
    unmatched_mic_count,
    includes_provider_bucket,
    is_enabled_for_instrument,
    is_enabled_for_eod_price,
    is_enabled_for_fundamental,
    is_enabled_for_ingestion,
    verification_symbol,
    last_verified_on,
    notes
FROM curated_namespace_codes
