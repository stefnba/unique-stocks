WITH price AS (
    SELECT *
    FROM {{ ref('stg_eod_price') }}
),

security AS (
    SELECT
        security_pk,
        data_provider,
        provider_symbol
    FROM {{ ref('dim_security') }}
),

final AS (
    SELECT
        {{ surrogate_key(["price.data_provider", "price.ticker", "price.bar_date"]) }} AS daily_price_pk,
        security.security_pk,
        price.data_provider,
        price.provider_exchange_code,
        price.ticker,
        price.open_price,
        price.high_price,
        price.low_price,
        price.close_price,
        price.adjusted_close_price,
        COALESCE(price.adjusted_close_price, price.close_price) AS effective_close_price,
        price.volume,
        price.bar_date,
        price.ingested_at
    FROM price
    LEFT JOIN security
        ON price.data_provider = security.data_provider
        AND price.ticker = security.provider_symbol
)

SELECT * FROM final
