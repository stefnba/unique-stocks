SELECT
    Code AS code,
    Name AS name,
    Currency AS currency,
    Type AS type,
    Isin AS isin,
    data_source
FROM
    $indices AS indices
