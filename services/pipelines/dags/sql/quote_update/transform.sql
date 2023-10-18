SELECT
    CAST("Date" AS date) AS "date",
    "Open" AS "open",
    "High" AS "high",
    "Low" AS "low",
    "Close" AS "close",
    "Adjusted_close" AS "adjusted_close",
    CAST("Volume" AS int) AS "volume",
    "Code" AS "security_code",
    "exchange" AS "exchange_code",
    CURRENT_DATE AS "created_at"
FROM
    $quotes_raw
