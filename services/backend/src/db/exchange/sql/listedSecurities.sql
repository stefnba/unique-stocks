SELECT
    sl.*,
    st.ticker,
    s."name",
    s.isin,
    s.security_type_id,
    s.figi AS share_class_figi,
    stp."name"
FROM
    data.security_listing sl
    LEFT JOIN data.security_ticker st ON sl.security_ticker_id = st.id
    LEFT JOIN data."security" s ON st.security_id = s.id
    LEFT JOIN data.security_type stp ON s.security_type_id = stp.id
