SELECT
    *,
    ticker AS idValue,
    'TICKER' AS idType
FROM
    df
WHERE
    isin IS NOT NULL
