WITH coverage AS (
    SELECT *
    FROM {{ ref('int_exchange_provider_coverage') }}
),

namespace_policy AS (
    SELECT
        LOWER(TRIM(CAST(data_provider AS VARCHAR))) AS data_provider,
        UPPER(TRIM(CAST(provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        LOWER(TRIM(CAST(universe_tier AS VARCHAR))) AS universe_tier,
        CAST(is_enabled_for_instrument AS BOOLEAN) AS is_enabled_for_instrument,
        CAST(is_enabled_for_eod_price AS BOOLEAN) AS is_enabled_for_eod_price,
        CAST(is_enabled_for_eod_backfill AS BOOLEAN) AS is_enabled_for_eod_backfill,
        CAST(is_enabled_for_fundamental AS BOOLEAN) AS is_enabled_for_fundamental,
        LOWER(TRIM(CAST(daily_coverage_mode AS VARCHAR))) AS daily_coverage_mode,
        LOWER(TRIM(CAST(historical_coverage_mode AS VARCHAR))) AS historical_coverage_mode,
        NULLIF(LOWER(TRIM(CAST(region AS VARCHAR))), '') AS region,
        CAST(priority AS INTEGER) AS policy_priority,
        NULLIF(TRIM(CAST(review_reason AS VARCHAR)), '') AS policy_reason,
        NULLIF(TRIM(CAST(owner AS VARCHAR)), '') AS policy_owner,
        CAST(last_reviewed_on AS DATE) AS policy_last_reviewed_on
    FROM {{ ref('provider_namespace_policy') }}
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
        FALSE AS is_enabled_for_instrument,
        FALSE AS is_enabled_for_eod_price,
        FALSE AS is_enabled_for_fundamental,
        FALSE AS is_enabled_for_ingestion,
        'catalog_only' AS universe_tier,
        FALSE AS is_enabled_for_eod_backfill,
        'none' AS daily_coverage_mode,
        'none' AS historical_coverage_mode,
        CAST(NULL AS VARCHAR) AS region,
        CAST(NULL AS INTEGER) AS policy_priority,
        CAST(NULL AS VARCHAR) AS policy_reason,
        CAST(NULL AS VARCHAR) AS policy_owner,
        CAST(NULL AS DATE) AS policy_last_reviewed_on,
        CAST(NULL AS VARCHAR) AS verification_symbol,
        CAST(NULL AS DATE) AS last_verified_on,
        CAST(NULL AS VARCHAR) AS notes
    FROM rolled_up
),

policy_catalog_codes AS (
    SELECT
        provider_catalog_codes.data_provider,
        provider_catalog_codes.provider_exchange_code,
        provider_catalog_codes.provider_code_kind,
        provider_catalog_codes.source_kind,
        provider_catalog_codes.exchange_catalog_name,
        provider_catalog_codes.asset_type_hint,
        provider_catalog_codes.country_iso2,
        provider_catalog_codes.currency,
        provider_catalog_codes.catalog_snapshot_date,
        provider_catalog_codes.catalog_code_count,
        provider_catalog_codes.coverage_candidate_count,
        provider_catalog_codes.mapped_mic_count,
        provider_catalog_codes.unmatched_mic_count,
        provider_catalog_codes.includes_provider_bucket,
        COALESCE(namespace_policy.is_enabled_for_instrument, FALSE) AS is_enabled_for_instrument,
        COALESCE(namespace_policy.is_enabled_for_eod_price, FALSE) AS is_enabled_for_eod_price,
        COALESCE(namespace_policy.is_enabled_for_eod_backfill, FALSE) AS is_enabled_for_eod_backfill,
        COALESCE(namespace_policy.is_enabled_for_fundamental, FALSE)
        AND provider_catalog_codes.mapped_mic_count > 0
            AS is_enabled_for_fundamental,
        (
            COALESCE(namespace_policy.is_enabled_for_instrument, FALSE)
            OR COALESCE(namespace_policy.is_enabled_for_eod_price, FALSE)
            OR COALESCE(namespace_policy.is_enabled_for_eod_backfill, FALSE)
            OR (
                COALESCE(namespace_policy.is_enabled_for_fundamental, FALSE)
                AND provider_catalog_codes.mapped_mic_count > 0
            )
        ) AS is_enabled_for_ingestion,
        COALESCE(namespace_policy.universe_tier, provider_catalog_codes.universe_tier) AS universe_tier,
        COALESCE(namespace_policy.daily_coverage_mode, provider_catalog_codes.daily_coverage_mode)
            AS daily_coverage_mode,
        COALESCE(namespace_policy.historical_coverage_mode, provider_catalog_codes.historical_coverage_mode)
            AS historical_coverage_mode,
        namespace_policy.region,
        namespace_policy.policy_priority,
        namespace_policy.policy_reason,
        namespace_policy.policy_owner,
        namespace_policy.policy_last_reviewed_on,
        provider_catalog_codes.verification_symbol,
        provider_catalog_codes.last_verified_on,
        provider_catalog_codes.notes
    FROM provider_catalog_codes
    LEFT JOIN namespace_policy
        ON provider_catalog_codes.data_provider = namespace_policy.data_provider
        AND provider_catalog_codes.provider_exchange_code = namespace_policy.provider_exchange_code
),

curated_namespace_codes AS (
    SELECT
        curated_provider_namespaces.data_provider,
        curated_provider_namespaces.provider_exchange_code,
        curated_provider_namespaces.provider_code_kind,
        'curated_seed' AS source_kind,
        curated_provider_namespaces.exchange_catalog_name,
        curated_provider_namespaces.asset_type_hint,
        curated_provider_namespaces.country_iso2,
        curated_provider_namespaces.currency,
        CAST(NULL AS DATE) AS catalog_snapshot_date,
        CAST(NULL AS INTEGER) AS catalog_code_count,
        0 AS coverage_candidate_count,
        0 AS mapped_mic_count,
        0 AS unmatched_mic_count,
        FALSE AS includes_provider_bucket,
        COALESCE(namespace_policy.is_enabled_for_instrument, FALSE) AS is_enabled_for_instrument,
        COALESCE(namespace_policy.is_enabled_for_eod_price, FALSE) AS is_enabled_for_eod_price,
        COALESCE(namespace_policy.is_enabled_for_eod_backfill, FALSE) AS is_enabled_for_eod_backfill,
        COALESCE(namespace_policy.is_enabled_for_fundamental, FALSE) AS is_enabled_for_fundamental,
        (
            COALESCE(namespace_policy.is_enabled_for_instrument, FALSE)
            OR COALESCE(namespace_policy.is_enabled_for_eod_price, FALSE)
            OR COALESCE(namespace_policy.is_enabled_for_eod_backfill, FALSE)
            OR COALESCE(namespace_policy.is_enabled_for_fundamental, FALSE)
        )
            AS is_enabled_for_ingestion,
        COALESCE(namespace_policy.universe_tier, 'disabled') AS universe_tier,
        COALESCE(namespace_policy.daily_coverage_mode, 'none') AS daily_coverage_mode,
        COALESCE(namespace_policy.historical_coverage_mode, 'none') AS historical_coverage_mode,
        namespace_policy.region,
        namespace_policy.policy_priority,
        namespace_policy.policy_reason,
        namespace_policy.policy_owner,
        namespace_policy.policy_last_reviewed_on,
        curated_provider_namespaces.verification_symbol,
        curated_provider_namespaces.last_verified_on,
        curated_provider_namespaces.notes
    FROM curated_provider_namespaces
    LEFT JOIN namespace_policy
        ON curated_provider_namespaces.data_provider = namespace_policy.data_provider
        AND curated_provider_namespaces.provider_exchange_code = namespace_policy.provider_exchange_code
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
    is_enabled_for_eod_backfill,
    is_enabled_for_fundamental,
    is_enabled_for_ingestion,
    universe_tier,
    daily_coverage_mode,
    historical_coverage_mode,
    region,
    policy_priority,
    policy_reason,
    policy_owner,
    policy_last_reviewed_on,
    verification_symbol,
    last_verified_on,
    notes
FROM policy_catalog_codes

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
    is_enabled_for_eod_backfill,
    is_enabled_for_fundamental,
    is_enabled_for_ingestion,
    universe_tier,
    daily_coverage_mode,
    historical_coverage_mode,
    region,
    policy_priority,
    policy_reason,
    policy_owner,
    policy_last_reviewed_on,
    verification_symbol,
    last_verified_on,
    notes
FROM curated_namespace_codes
