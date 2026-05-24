SELECT
    ticker,
    exchange_code,
    bar_date,
    open_price,
    high_price,
    low_price,
    close_price,
    adjusted_close_price,
    COALESCE(adjusted_close_price, close_price) AS effective_close_price,
    volume,
    provider,
    row_hash,
    source_uri,
    ingested_at
FROM {{ ref('stg_eod_prices') }}
