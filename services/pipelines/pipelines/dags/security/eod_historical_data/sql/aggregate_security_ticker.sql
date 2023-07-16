SELECT
    security_ticker_id AS id,
    security_id,
    ticker_figi
FROM
    $security_ticker
GROUP BY
    security_ticker_id,
    security_id,
    ticker_figi
