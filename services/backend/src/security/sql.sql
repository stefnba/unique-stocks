SELECT
    security_listing.*,
    security_ticker.ticker,
    exchange. "name",
    exchange. "mic",
    security.*
FROM
    security_listing
    LEFT JOIN exchange ON exchange_id = exchange.id
    LEFT JOIN security_ticker ON security_ticker_id = security_ticker.id
    LEFT JOIN "security" ON security_ticker.security_id = "security".id
WHERE
    security_ticker.ticker = 'AAPL'
