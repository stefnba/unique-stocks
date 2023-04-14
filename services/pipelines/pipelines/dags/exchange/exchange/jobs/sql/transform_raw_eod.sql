SELECT
    exchange_code_mapping.uid AS exchange_uid,
    exchange_code_mapping.uid AS mic,
    exchange.Code AS exchange_source_code,
    exchange.Name AS exchange_name,
    NULL AS acronym,
    exchange.Currency AS currency,
    NULL AS city,
    exchange.CountryISO2 AS country,
    NULL AS website,
    COALESCE(virtual_exchange_mapping.uid, 0) AS is_virtual,
    $source AS data_source
FROM
    $exchange AS exchange
    LEFT JOIN $exchange_code_mapping AS exchange_code_mapping ON exchange.Code = exchange_code_mapping.source_value
    LEFT JOIN $virtual_exchange_mapping AS virtual_exchange_mapping ON exchange.Code = virtual_exchange_mapping.source_value
WHERE
    exchange.Code NOT IN ('IL', 'VX')
