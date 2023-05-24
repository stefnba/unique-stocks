/*
FULL JOIN since eod and msk have virtual exchanges
 */
SELECT
    -- "data_iso"."mic" AS "mic",
    COALESCE("data_iso"."mic", "data_eod"."mic", "data_msk"."mic") AS "mic",
    "data_iso"."operating_mic" AS "operating_mic",
    --COALESCE("data_msk"."uid", "data_eod"."uid", "data_iso"."uid") AS "uid",
    COALESCE("data_msk"."name", "data_eod"."name", "data_iso"."name") AS "name",
    COALESCE("data_msk"."acronym", "data_iso"."acronym") AS "acronym",
    COALESCE("data_msk"."website", "data_iso"."website") AS "website",
    COALESCE(data_msk.city, data_iso.city, data_eod.city) AS "city",
    COALESCE(data_msk.country, data_eod.country, data_iso.country) AS country,
    COALESCE(data_msk.currency, data_eod.currency) AS currency,
    COALESCE(data_msk.timezone, data_eod.timezone) AS timezone,
    COALESCE("data_eod"."source", "data_msk"."source", "data_iso"."source") AS "source",
    CAST(COALESCE("data_eod"."is_virtual", "data_msk"."is_virtual", 0) AS INT) AS "is_virtual",
    COALESCE("data_iso"."comment") AS "comment",
    COALESCE("data_iso"."status") AS "status",
    -- data_eod.working_days,
    -- data_eod.trading_hours,
    -- data_eod.active_tickers,
    -- data_eod.updated_tickers,
    -- data_eod.previous_day_updated_tickers
FROM
    $data_iso AS data_iso
    LEFT JOIN $data_eod AS data_eod ON data_iso.uid = data_eod.uid
    LEFT JOIN $data_msk AS data_msk ON data_iso.uid = data_msk.uid
