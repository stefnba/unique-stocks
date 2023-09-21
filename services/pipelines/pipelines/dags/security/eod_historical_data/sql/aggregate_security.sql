-- WITH cte AS (
--     SELECT
--         security_uid,
--         FIRST (security_type_id) security_type_id,
--         FIRST (security_id) id,
--         FIRST (entity_isin_id) entity_isin_id,
--         FIRST (level_figi) level_figi,
--         FIRST (name_figi) name_figi,
--         LIST_DISTINCT(LIST (isin)) isin,
--         LIST_DISTINCT(LIST (name_figi)) name_figi_alias
--     FROM
--         $security
--     GROUP BY
--         security_uid
-- )
-- SELECT
--     *
--     EXCLUDE (isin, name_figi_alias),
--     LIST_FILTER(name_figi_alias, x -> x <> name_figi) name_figi_alias,
--     LIST_FILTER(isin, x -> x IS NOT NULL)[1] isin
-- FROM
--     cte
WITH security_level AS (
    SELECT
        security_id AS id,
        security_uid AS figi,
        FIRST (security_type_id) security_type_id,
        -- MAX(level_figi) level_figi,
        FIRST (name_figi) AS "name",
        LIST_DISTINCT(LIST (isin)) isin,
        LIST_DISTINCT(LIST (name_figi)) name_figi_alias
    FROM
        $security
    GROUP BY
        security_id,
        security_uid
)
SELECT
    *
    EXCLUDE (isin, name_figi_alias),
    LIST_FILTER(name_figi_alias, x -> x <> name) name_figi_alias,
    LIST_FILTER(isin, x -> x IS NOT NULL)[1] isin
FROM
    security_level
