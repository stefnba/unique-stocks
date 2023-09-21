SELECT
    "date"::text AS "timestamp",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close" AS "adj_close",
    "volume",
    $security_listing_id AS "security_listing_id",
    $interval_id AS "interval_id"
FROM
    $data
