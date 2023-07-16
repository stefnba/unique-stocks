WITH cte AS (
    SELECT
        security_uid,
        FIRST (security_type_id) security_type_id,
        FIRST (security_id) id,
        FIRST (level_figi) level_figi,
        FIRST (name_figi) name_figi,
        LIST_DISTINCT(LIST (isin)) isin,
        LIST_DISTINCT(LIST (name_figi)) name_figi_alias
    FROM
        $security
    GROUP BY
        security_uid
)
SELECT
    *
    EXCLUDE (isin, name_figi_alias),
    LIST_FILTER(name_figi_alias, x -> x <> name_figi) name_figi_alias,
    LIST_FILTER(isin, x -> x IS NOT NULL)[1] isin
FROM
    cte
