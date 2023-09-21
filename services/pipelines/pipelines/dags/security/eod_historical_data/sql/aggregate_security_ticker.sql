SELECT
    security_ticker_uid,
    security_id,
    ticker
FROM
    $security_ticker
GROUP BY
    security_ticker_uid,
    security_id,
    ticker
