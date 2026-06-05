{{ config(tags=['ingestion_control']) }}

WITH instrument_universe AS (
    SELECT *
    FROM {{ ref('int_latest_instrument_universe') }}
),

completion_ranges AS (
    SELECT *
    FROM {{ ref('int_eod_price_completion_ranges') }}
),

final AS (
    SELECT
        instrument_universe.data_provider,
        instrument_universe.provider_exchange_code,
        instrument_universe.provider_symbol,
        instrument_universe.instrument_family,
        completion_ranges.min_bar_date,
        completion_ranges.max_bar_date,
        completion_ranges.bar_count
    FROM instrument_universe
    LEFT JOIN completion_ranges
        ON instrument_universe.data_provider = completion_ranges.data_provider
        AND instrument_universe.provider_exchange_code = completion_ranges.provider_exchange_code
        AND instrument_universe.provider_symbol = completion_ranges.provider_symbol
    WHERE instrument_universe.data_provider = 'eodhd'
)

SELECT * FROM final
