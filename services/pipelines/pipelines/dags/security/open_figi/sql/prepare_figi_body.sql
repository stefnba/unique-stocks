SELECT
    *,
    CASE WHEN isin IS NOT NULL THEN
        isin
    WHEN ticker IS NOT NULL THEN
        ticker
    ELSE
        NULL
    END AS idValue,
    CASE WHEN isin IS NOT NULL THEN
        'ID_ISIN'
    WHEN ticker IS NOT NULL THEN
        'TICKER'
    ELSE
        NULL
    END AS idType
FROM
    df
