SELECT
    exchange_code_mappings.uid AS exchange_uid,
    exchange_code_mappings.uid AS mic,
    exchanges.Code AS exchange_source_code,
    exchanges.Name AS exchange_name,
    NULL AS acronym,
    exchanges.Currency AS currency,
    NULL AS city,
    exchanges.CountryISO2 AS country,
    NULL AS website,
    COALESCE(virtual_exchange_mappings.uid, 0) AS is_virtual,
    $source AS data_source
FROM
    $exchanges AS exchanges
    LEFT JOIN $exchange_code_mappings AS exchange_code_mappings ON exchanges.Code = exchange_code_mappings.source_value
    LEFT JOIN $virtual_exchange_mappings AS virtual_exchange_mappings ON exchanges.Code = virtual_exchange_mappings.source_value
WHERE
    exchanges.Code NOT IN ('IL', 'VX')
