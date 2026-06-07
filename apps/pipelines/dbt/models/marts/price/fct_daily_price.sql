WITH price AS (
    SELECT *
    FROM {{ ref('stg_eod_price') }}
),

instrument AS (
    SELECT
        instrument_pk,
        data_provider,
        provider_exchange_code,
        provider_instrument_code
    FROM {{ ref('dim_instrument') }}
),

final AS (
    SELECT
        {{ surrogate_key([
            "price.data_provider",
            "price.provider_exchange_code",
            "price.provider_instrument_code",
            "price.bar_date",
        ]) }}
            AS daily_price_pk,
        instrument.instrument_pk,
        price.data_provider,
        price.provider_exchange_code,
        price.provider_instrument_code,
        price.ingestion_mode,
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
        AND price.provider_exchange_code = instrument.provider_exchange_code
        AND price.provider_instrument_code = instrument.provider_instrument_code
)

SELECT * FROM final
