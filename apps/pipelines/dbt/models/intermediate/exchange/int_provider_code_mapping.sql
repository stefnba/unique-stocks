WITH provider_universe AS (
    SELECT *
    FROM {{ ref('int_exchange_provider_ingestion_universe') }}
),

coverage AS (
    SELECT *
    FROM {{ ref('int_exchange_provider_coverage') }}
),

latest_schedule AS (
    SELECT *
    FROM {{ ref('stg_exchange_schedule') }}
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY data_provider, provider_schedule_exchange_code
            ORDER BY snapshot_date DESC, ingested_at DESC, ingestion_id DESC
        ) = 1
),

request_code_mapping AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_code_kind,
        source_kind,
        exchange_catalog_name,
        'provider_exchange_code' AS mapping_type,
        provider_exchange_code AS mapping_code,
        'self' AS mapping_method,
        'high' AS mapping_confidence
    FROM provider_universe
),

mic_mapping AS (
    SELECT
        coverage.data_provider,
        coverage.provider_exchange_code,
        provider_universe.provider_code_kind,
        provider_universe.source_kind,
        coverage.exchange_catalog_name,
        'mic' AS mapping_type,
        coverage.mic AS mapping_code,
        coverage.match_method AS mapping_method,
        coverage.match_confidence AS mapping_confidence
    FROM coverage
    LEFT JOIN provider_universe
        ON coverage.data_provider = provider_universe.data_provider
        AND coverage.provider_exchange_code = provider_universe.provider_exchange_code
    WHERE coverage.mic IS NOT NULL
),

schedule_exact_mapping AS (
    SELECT
        provider_universe.data_provider,
        provider_universe.provider_exchange_code,
        provider_universe.provider_code_kind,
        provider_universe.source_kind,
        provider_universe.exchange_catalog_name,
        'schedule_code' AS mapping_type,
        latest_schedule.provider_schedule_exchange_code AS mapping_code,
        'provider_code_exact' AS mapping_method,
        'high' AS mapping_confidence
    FROM provider_universe
    INNER JOIN latest_schedule
        ON provider_universe.data_provider = latest_schedule.data_provider
        AND provider_universe.provider_exchange_code = latest_schedule.provider_schedule_exchange_code
),

schedule_mic_mapping AS (
    SELECT
        coverage.data_provider,
        coverage.provider_exchange_code,
        provider_universe.provider_code_kind,
        provider_universe.source_kind,
        coverage.exchange_catalog_name,
        'schedule_code' AS mapping_type,
        latest_schedule.provider_schedule_exchange_code AS mapping_code,
        'mic_exact' AS mapping_method,
        'medium' AS mapping_confidence
    FROM coverage
    INNER JOIN latest_schedule
        ON coverage.data_provider = latest_schedule.data_provider
        AND (
            coverage.mic = latest_schedule.provider_schedule_exchange_code
            OR coverage.operating_mic = latest_schedule.provider_schedule_exchange_code
        )
    LEFT JOIN provider_universe
        ON coverage.data_provider = provider_universe.data_provider
        AND coverage.provider_exchange_code = provider_universe.provider_exchange_code
),

unioned AS (
    SELECT * FROM request_code_mapping
    UNION ALL
    SELECT * FROM mic_mapping
    UNION ALL
    SELECT * FROM schedule_exact_mapping
    UNION ALL
    SELECT * FROM schedule_mic_mapping
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY data_provider, provider_exchange_code, mapping_type, mapping_code
            ORDER BY
                CASE mapping_confidence
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    ELSE 3
                END,
                mapping_method
        ) AS row_number
    FROM unioned
)

SELECT
    data_provider
    || ':'
    || provider_exchange_code
    || ':'
    || mapping_type
    || ':'
    || mapping_code AS provider_code_mapping_id,
    data_provider,
    provider_exchange_code,
    provider_code_kind,
    source_kind,
    exchange_catalog_name,
    mapping_type,
    mapping_code,
    mapping_method,
    mapping_confidence
FROM deduplicated
WHERE row_number = 1
