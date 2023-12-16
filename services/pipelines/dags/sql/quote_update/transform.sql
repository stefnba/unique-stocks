SELECT
    CAST("Date" AS date) AS "date",
    "Open" AS "open",
    "High" AS "high",
    "Low" AS "low",
    "Close" AS "close",
    "Adjusted_close" AS "adjusted_close",
    CAST("Volume" AS bigint) AS "volume",
    "Code" AS "security_code",
    "exchange" AS "exchange_code",
    year("date") AS "year",
    CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS "created_at",
FROM
    $quotes_raw
