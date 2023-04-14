SELECT
    exchange.exchange_uid,
    exchange.exchange_source_code,
    exchange.exchange_name,
    exchange.acronym,
    exchange.currency,
    exchange.city,
    exchange.country,
    exchange.website,
    exchange.is_virtual,
    exchange.data_source,
    details.timezone,
    details.working_days,
    details.trading_hours,
    details.active_tickers,
    details.updated_tickers,
    details.previous_day_updated_tickers
FROM
    $exchange AS exchange
    LEFT JOIN $details AS details ON exchange.exchange_uid = details.exchange_uid
