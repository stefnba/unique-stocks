SELECT
    "Date" AS "date",
    "Open" AS "open",
    "High" AS "high",
    "Low" AS "low",
    "Close" AS "close",
    "Adjusted_close" AS "adjusted_close",
    "Volume" AS "volume",
    "security" AS "security_code",
    "exchange" AS "exchange_code"
FROM
    $quotes_raw
