{{ config(tags=['ingestion_control']) }}

WITH price AS (
    SELECT *
    FROM {{ ref('stg_eod_price') }}
),

final AS (
    SELECT
        data_provider,
        provider_exchange_code,
        ticker AS provider_symbol,
        MIN(bar_date) AS min_bar_date,
        MAX(bar_date) AS max_bar_date,
        COUNT(*) AS bar_count
    FROM price
    GROUP BY 1, 2, 3
)

SELECT * FROM final
