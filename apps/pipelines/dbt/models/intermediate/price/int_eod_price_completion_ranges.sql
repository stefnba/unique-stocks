{{ config(tags=['ingestion_control']) }}

WITH instrument_history_bounds AS (
    SELECT *
    FROM {{ ref('int_eod_price_instrument_history_bounds') }}
),

final AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_instrument_code,
        first_observed_price_date AS min_bar_date,
        last_observed_price_date AS max_bar_date,
        observed_price_days AS bar_count
    FROM instrument_history_bounds
)

SELECT * FROM final
