{{ config(tags=['ingestion_control']) }}

WITH price AS (
    SELECT *
    FROM {{ ref('stg_eod_price') }}
),

final AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_instrument_code,
        MIN(bar_date) AS first_observed_price_date,
        MAX(bar_date) AS last_observed_price_date,
        COUNT(*) AS observed_price_days
    FROM price
    GROUP BY 1, 2, 3
)

SELECT * FROM final
