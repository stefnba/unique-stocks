SELECT
    *,
    -- since some securities don't have share class figi, we must replace those with composite figi
    COALESCE("share_class_figi", "composite_figi") AS "security_uid",
    IF ("share_class_figi" IS NOT NULL,
        0,
        1) AS "level_figi",
    "security_uid" || '_' || "ticker_figi" AS "security_ticker_uid"
FROM
    $security
