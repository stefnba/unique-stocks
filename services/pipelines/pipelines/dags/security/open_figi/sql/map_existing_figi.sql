SELECT
    securities.*,
    COALESCE(mapping_figi_isin.figi, mapping_figi_ticker.figi) AS figi,
    COALESCE(mapping_figi_isin.share_class_figi, mapping_figi_ticker.share_class_figi) AS share_class_figi,
    COALESCE(mapping_figi_isin.composite_figi, mapping_figi_ticker.composite_figi) AS composite_figi
FROM
    $securities AS securities
    LEFT JOIN $mapping_figi AS mapping_figi_isin ON securities.isin = mapping_figi_isin.isin
    LEFT JOIN $mapping_figi AS mapping_figi_ticker ON securities.ticker = mapping_figi_ticker.ticker
