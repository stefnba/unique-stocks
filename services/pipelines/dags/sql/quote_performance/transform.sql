WITH unique_security AS (
    SELECT DISTINCT
        security_code,
        exchange_code
    FROM
        $quote_data
),
date_table AS (
    SELECT
        CAST(RANGE AS date) AS date
FROM
    RANGE (CURRENT_DATE - INTERVAL 10 YEAR - 10,
        CURRENT_DATE,
        interval 1 DAY)
),
security_base AS (
    SELECT
        *
    FROM
        unique_security,
        date_table
),
quote_base AS (
    SELECT
        b.security_code,
        b.exchange_code,
        b.date,
        IFNULL(s."adjusted_close", LEAD(s."adjusted_close", 1 IGNORE NULLS) OVER (PARTITION BY b.security_code, b.exchange_code ORDER BY b."date" DESC)) AS "close"
FROM
    security_base b
    LEFT JOIN $quote_data s ON b.security_code = s.security_code
        AND b.exchange_code = s.exchange_code
        AND b.date = s.date
),
performance AS (
    SELECT
        b."security_code",
        b."exchange_code",
        b."date" AS base_date,
        r."date" AS reference_date,
        b."close" AS base_close,
        r."close" AS reference_close,
        'YEAR_TO_DATE' AS period,
(base_close / reference_close - 1) AS performance,
FROM
    quote_base b
    LEFT JOIN quote_base r ON date_trunc('year', b."date") = r."date"
        AND b.security_code = r.security_code
        AND b.exchange_code = r.exchange_code
    WHERE
        b."date" = '2023-10-12'
    UNION ALL
    SELECT
        b."security_code",
        b."exchange_code",
        b."date" AS base_date,
        r."date" AS reference_date,
        b."close" AS base_close,
        r."close" AS reference_close,
        'MONTH_TO_DATE' AS period,
(base_close / reference_close - 1) AS performance,
    FROM
        quote_base b
        LEFT JOIN quote_base r ON date_trunc('month', b."date") = r."date"
            AND b.security_code = r.security_code
            AND b.exchange_code = r.exchange_code
    WHERE
        b."date" = '2023-10-12'
    UNION ALL
    SELECT
        b."security_code",
        b."exchange_code",
        b."date" AS base_date,
        r."date" AS reference_date,
        b."close" AS base_close,
        r."close" AS reference_close,
        'LAST_3_MONTH' AS period,
(base_close / reference_close - 1) AS performance,
    FROM
        quote_base b
        LEFT JOIN quote_base r ON b."date" - INTERVAL 1 MONTH = r."date"
            AND b.security_code = r.security_code
            AND b.exchange_code = r.exchange_code
    WHERE
        b."date" = '2023-10-12'
    UNION ALL
    SELECT
        b."security_code",
        b."exchange_code",
        b."date" AS base_date,
        r."date" AS reference_date,
        b."close" AS base_close,
        r."close" AS reference_close,
        'LAST_12_MONTH' AS period,
(base_close / reference_close - 1) AS performance,
    FROM
        quote_base b
        LEFT JOIN quote_base r ON b."date" - INTERVAL 12 MONTH = r."date"
            AND b.security_code = r.security_code
            AND b.exchange_code = r.exchange_code
    WHERE
        b."date" = '2023-10-12'
    UNION ALL
    SELECT
        b."security_code",
        b."exchange_code",
        b."date" AS base_date,
        r."date" AS reference_date,
        b."close" AS base_close,
        r."close" AS reference_close,
        'LAST_6_MONTH' AS period,
(base_close / reference_close - 1) AS performance,
    FROM
        quote_base b
        LEFT JOIN quote_base r ON b."date" - INTERVAL 6 MONTH = r."date"
            AND b.security_code = r.security_code
            AND b.exchange_code = r.exchange_code
    WHERE
        b."date" = '2023-10-12'
    UNION ALL
    SELECT
        b."security_code",
        b."exchange_code",
        b."date" AS base_date,
        r."date" AS reference_date,
        b."close" AS base_close,
        r."close" AS reference_close,
        'LAST_1_MONTH' AS period,
(base_close / reference_close - 1) AS performance,
    FROM
        quote_base b
        LEFT JOIN quote_base r ON b."date" - INTERVAL 1 MONTH = r."date"
            AND b.security_code = r.security_code
            AND b.exchange_code = r.exchange_code
    WHERE
        b."date" = '2023-10-12'
    UNION ALL
    SELECT
        b."security_code",
        b."exchange_code",
        b."date" AS base_date,
        r."date" AS reference_date,
        b."close" AS base_close,
        r."close" AS reference_close,
        'LAST_2_YEAR' AS period,
(base_close / reference_close - 1) AS performance,
    FROM
        quote_base b
        LEFT JOIN quote_base r ON b."date" - INTERVAL 2 YEAR = r."date"
            AND b.security_code = r.security_code
            AND b.exchange_code = r.exchange_code
    WHERE
        b."date" = '2023-10-12'
    UNION ALL
    SELECT
        b."security_code",
        b."exchange_code",
        b."date" AS base_date,
        r."date" AS reference_date,
        b."close" AS base_close,
        r."close" AS reference_close,
        'LAST_5_YEAR' AS period,
(base_close / reference_close - 1) AS performance,
    FROM
        quote_base b
        LEFT JOIN quote_base r ON b."date" - INTERVAL 5 YEAR = r."date"
            AND b.security_code = r.security_code
            AND b.exchange_code = r.exchange_code
    WHERE
        b."date" = '2023-10-12'
    UNION ALL
    SELECT
        b."security_code",
        b."exchange_code",
        b."date" AS base_date,
        r."date" AS reference_date,
        b."close" AS base_close,
        r."close" AS reference_close,
        'LAST_10_YEAR' AS period,
(base_close / reference_close - 1) AS performance,
    FROM
        quote_base b
        LEFT JOIN quote_base r ON b."date" - INTERVAL 10 YEAR = r."date"
            AND b.security_code = r.security_code
            AND b.exchange_code = r.exchange_code
    WHERE
        b."date" = '2023-10-12'
)
SELECT
    *
FROM
    performance
ORDER BY
    security_code,
    period
