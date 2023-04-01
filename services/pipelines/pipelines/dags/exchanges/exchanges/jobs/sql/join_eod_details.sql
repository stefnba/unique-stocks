SELECT
    exchanges.exchange_uid,
    exchanges.exchange_source_code,
    exchanges.exchange_name,
    exchanges.acronym,
    exchanges.currency,
    exchanges.city,
    exchanges.country,
    exchanges.website,
    exchanges.is_virtual,
    exchanges.data_source,
    details.timezone,
    details.working_days,
    details.trading_hours,
    details.active_tickers,
    details.updated_tickers,
    details.previous_day_updated_tickers
FROM
    $exchanges AS exchanges
    LEFT JOIN $details AS details ON exchanges.exchange_uid = details.exchange_uid
