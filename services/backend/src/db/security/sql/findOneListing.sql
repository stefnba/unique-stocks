SELECT
    security_listing.id,
    security_listing.figi,
    security_ticker.ticker,
    security_listing.quote_source,
    security_listing.currency,
    to_json(exchange) exchange,
    COALESCE((
        SELECT
            json_agg(json_build_object('close', "close", 'date', "timestamp", 'high', "high", 'low', "low", 'adj_close', "adj_close"))
        FROM data.security_quote
        WHERE
            security_quote.security_listing_id = security_listing.id
            AND interval_id = 1), '[]'::json) quotes
FROM
    data. "security_listing"
    LEFT JOIN (
        SELECT
            id,
            name,
            mic
        FROM
            data.exchange) exchange ON security_listing.exchange_id = exchange.id
    LEFT JOIN data.security_ticker ON security_ticker_id = security_ticker.id
