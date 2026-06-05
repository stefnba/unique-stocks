{{ config(tags=['ingestion_control']) }}

WITH coverage AS (
    SELECT *
    FROM {{ ref('stg_pipeline_ingestion_coverage') }}
),

final AS (
    SELECT
        data_provider,
        JSON_EXTRACT_STRING(unit_key_json, '$."provider_exchange_code"') AS provider_exchange_code,
        JSON_EXTRACT_STRING(unit_key_json, '$."ticker"') AS provider_symbol,
        CAST(JSON_EXTRACT_STRING(unit_key_json, '$."from_date"') AS DATE) AS from_date,
        CAST(JSON_EXTRACT_STRING(unit_key_json, '$."to_date"') AS DATE) AS to_date,
        unit_key_hash,
        status,
        reason,
        rows_raw,
        rows_valid,
        rows_rejected,
        source_uri,
        recorded_at
    FROM coverage
    WHERE domain = 'eod_price'
        AND data_provider = 'eodhd'
        AND unit_type = 'ticker_backfill'
        AND status IN ('completed', 'no_data')
)

SELECT * FROM final
