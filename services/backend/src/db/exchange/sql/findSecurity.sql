SELECT
    st.security_id,
    sl.security_ticker_id,
    sl.id AS security_listing_id,
    st.ticker,
    s. "name",
    s. "isin",
    s.security_type_id
FROM
    "data"."security_listing" AS sl
    LEFT JOIN "data"."security_ticker" AS st ON sl. "security_ticker_id" = st."id"
    LEFT JOIN data. "security" s ON st. "security_id" = s."id"
