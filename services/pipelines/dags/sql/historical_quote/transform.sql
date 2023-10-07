SELECT
    "Date" AS "date",
    "Open" AS "open",
    "High" AS "high",
    "Low" AS "low",
    "Close" AS "close",
    "Adjusted_close" AS "adjusted_close",
    "Volume" AS "volume",
    "security_code",
    "exchange_code"
FROM
    $quotes_raw
