/*
 */
WITH unique_security_name AS (
    SELECT
        isin,
        security_uid,
        security_type_id,
        name_figi,
        COUNT(*) AS "count",
        level_figi
    FROM
        $security
    GROUP BY
        isin,
        security_uid,
        security_type_id,
        level_figi,
        name_figi
    ORDER BY
        count DESC
),
unique_security AS (
    SELECT
        security_uid,
        security_type_id,
        level_figi,
        FIRST (name_figi) AS name_figi,
        COUNT(DISTINCT name_figi) AS name_figi_count,
        LIST (name_figi) AS name_figi_alias,
        LIST (isin) isin
    FROM
        unique_security_name
    GROUP BY
        security_uid,
        security_type_id,
        level_figi)
    /*
     */
    SELECT
        *
        EXCLUDE (name_figi_alias),
        LIST_FILTER(name_figi_alias, x -> x <> name_figi) name_figi_alias,
        LIST_FILTER(isin, x -> x IS NOT NULL)[1] isin
FROM
    unique_security
