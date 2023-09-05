SELECT
    *,
    $security_code AS security_code,
    $exchange_code AS exchange_code
FROM
    $quotes_raw
