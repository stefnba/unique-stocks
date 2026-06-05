{{ config(tags=['ingestion_control']) }}

WITH instrument_universe AS (
    SELECT *
    FROM {{ ref('int_latest_instrument_universe') }}
),

final AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_instrument_code,
        instrument_family
    FROM instrument_universe
    WHERE data_provider = 'eodhd'
)

SELECT * FROM final
