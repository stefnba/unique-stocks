SELECT
    isin,
    wkn,
    ticker,
    ticker_figi,
    exchange_mic AS exchange_mic_figi,
    name_figi,
    currency,
    country,
    figi,
    share_class_figi,
    composite_figi,
    security_type_id
FROM
    mapping.figi
