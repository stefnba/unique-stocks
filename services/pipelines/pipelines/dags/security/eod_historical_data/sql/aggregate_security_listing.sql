-- WITH cte AS (
--     SELECT
--         security_listing_id AS id,
--         figi,
--         LIST (exchange_id) exchange_id,
--         LIST (security_ticker_id) security_ticker_id,
--         LIST (currency) currency,
--         LIST (quote_source) quote_source
--     FROM
--         $security_listing
--     GROUP BY
--         security_listing_id,
--         figi
-- )
-- SELECT
--     *
--     EXCLUDE (exchange_id, currency, quote_source),
--     LIST_FILTER(exchange_id, x -> x IS NOT NULL)[1] exchange_id,
--     LIST_FILTER(security_ticker_id, x -> x IS NOT NULL)[1] security_ticker_id,
--     LIST_FILTER(currency, x -> x IS NOT NULL)[1] currency,
--     LIST_FILTER(quote_source, x -> x IS NOT NULL)[1] quote_source,
-- FROM
--     cte
SELECT
    "security_ticker_uid",
    "figi",
    "exchange_uid",
    MAX("quote_source") "quote_source",
    max("currency") "currency"
FROM
    $security_listing
GROUP BY
    "security_ticker_uid",
    "figi",
    "exchange_uid"
