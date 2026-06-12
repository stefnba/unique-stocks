{{ config(tags=['ingestion_control']) }}

WITH namespace_policy AS (
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

provider_universe AS (
    SELECT *
    FROM {{ ref('int_exchange_provider_ingestion_universe') }}
)

SELECT
    namespace_policy.data_provider
    || ':'
    || namespace_policy.provider_exchange_code AS provider_policy_resolution_id,
    namespace_policy.data_provider,
    namespace_policy.provider_exchange_code,
    namespace_policy.universe_tier,
    namespace_policy.is_enabled_for_instrument,
    namespace_policy.is_enabled_for_eod_price,
    namespace_policy.is_enabled_for_eod_backfill,
    namespace_policy.is_enabled_for_fundamental,
    namespace_policy.daily_coverage_mode,
    namespace_policy.historical_coverage_mode,
    namespace_policy.region,
    namespace_policy.policy_priority,
    namespace_policy.policy_reason,
    namespace_policy.policy_owner,
    namespace_policy.policy_last_reviewed_on,
    provider_universe.provider_exchange_code IS NOT NULL AS is_resolved_in_provider_universe,
    CASE
        WHEN provider_universe.provider_exchange_code IS NOT NULL THEN 'resolved'
        ELSE 'missing_provider_catalog_or_curated_namespace'
    END AS resolution_status,
    provider_universe.provider_code_kind AS resolved_provider_code_kind,
    provider_universe.source_kind AS resolved_source_kind,
    provider_universe.exchange_catalog_name AS resolved_exchange_catalog_name,
    provider_universe.is_enabled_for_instrument AS resolved_is_enabled_for_instrument,
    provider_universe.is_enabled_for_eod_price AS resolved_is_enabled_for_eod_price,
    provider_universe.is_enabled_for_eod_backfill AS resolved_is_enabled_for_eod_backfill,
    provider_universe.is_enabled_for_fundamental AS resolved_is_enabled_for_fundamental
FROM namespace_policy
LEFT JOIN provider_universe
    ON namespace_policy.data_provider = provider_universe.data_provider
    AND namespace_policy.provider_exchange_code = provider_universe.provider_exchange_code
