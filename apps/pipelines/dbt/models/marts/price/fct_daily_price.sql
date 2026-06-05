WITH price AS (
    SELECT *
    FROM {{ ref('stg_eod_price') }}
),

instrument AS (
    SELECT
        instrument_pk,
        data_provider,
        provider_symbol
    FROM {{ ref('dim_instrument') }}
),

final AS (
    SELECT
        {{ surrogate_key(["price.data_provider", "price.ticker", "price.bar_date"]) }} AS daily_price_pk,
        instrument.instrument_pk,
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
    LEFT JOIN instrument
        ON price.data_provider = instrument.data_provider
        AND price.ticker = instrument.provider_symbol
)

SELECT * FROM final
