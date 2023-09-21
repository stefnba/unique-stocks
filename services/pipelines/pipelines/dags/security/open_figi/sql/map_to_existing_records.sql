SELECT
    data.*,
    mapping_figi.*
    EXCLUDE (isin_source, ticker_source, wkn_source)
FROM
    $data data
    LEFT JOIN $mapping_figi mapping_figi ON data.isin = mapping_figi.isin_source
UNION
SELECT
    data.*,
    mapping_figi.*
    EXCLUDE (isin_source, ticker_source, wkn_source)
FROM
    $data data
    LEFT JOIN $mapping_figi mapping_figi ON data.ticker = mapping_figi.ticker_source;

