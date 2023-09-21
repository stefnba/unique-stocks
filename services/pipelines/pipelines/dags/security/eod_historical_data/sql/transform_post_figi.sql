SELECT
    "security"."ticker_figi" AS "ticker",
    "security"."isin",
    "security"."security_type_id",
    "security"."name_figi",
    "security"."figi",
    -- "security"."country",
    -- "security"."currency",
    "security"."exchange_code_figi",
    -- since some securities don't have share class figi, we must replace those with composite figi
    "share_class_figi" AS "security_uid",
    -- COALESCE("share_class_figi", "composite_figi") AS "security_uid",
    -- IF ("share_class_figi" IS NOT NULL,
    --     0,
    --     1) AS "level_figi",
    "security_uid" || '_' || "ticker_figi" AS "security_ticker_uid",
    "exchange_mapping"."uid" AS "exchange_uid",
    IF ("ticker" = "ticker_figi" AND "security"."exchange_uid" = "exchange_mapping"."uid",
        "security"."source",
        NULL) AS "quote_source",
    IF ("quote_source" IS NOT NULL,
        "currency",
        NULL) AS "currency",
FROM
    $security AS "security"
    LEFT JOIN $exchange_mapping AS "exchange_mapping" ON "exchange_mapping"."source_value" = "security"."exchange_code_figi"
WHERE
    "share_class_figi" IS NOT NULL
