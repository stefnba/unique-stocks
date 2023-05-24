SELECT
    "data_iso"."mic" AS "mic",
    COALESCE("data_msk"."exchange_uid", "data_eod"."exchange_uid", "data_iso"."mic") AS "uid",
    COALESCE("data_msk"."exchange_name", "data_eod"."exchange_name", "data_iso"."exchange_name") AS "name",
    COALESCE("data_msk"."acronym", "data_iso"."acronym") AS "acronym",
    COALESCE("data_msk"."website", "data_iso"."website") AS "website",
    COALESCE(data_msk.city, data_iso.city, data_eod.city) AS "city",
    COALESCE(data_msk.country, data_eod.country, data_iso.country) AS country,
    COALESCE(data_msk.currency, data_eod.currency) AS currency,
    COALESCE(data_msk.timezone, data_eod.timezone) AS timezone,
    COALESCE("data_eod"."data_source", "data_msk"."data_source", "data_iso"."data_source") AS "data_source",
    COALESCE("data_eod"."is_virtual", "data_msk"."is_virtual") AS "is_virtual",
    COALESCE("data_iso"."comments") AS "comments",
    data_eod.working_days,
    data_eod.trading_hours,
    data_eod.active_tickers,
    data_eod.updated_tickers,
    data_eod.previous_day_updated_tickers
FROM
    $data_iso AS data_iso
    LEFT JOIN $data_eod AS data_eod ON data_iso.exchange_uid = data_eod.exchange_uid
    LEFT JOIN $data_msk AS data_msk ON data_iso.exchange_uid = data_msk.exchange_uid
