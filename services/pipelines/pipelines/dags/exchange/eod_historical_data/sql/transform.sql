SELECT
    IF (exchange.Code = 'US',
        exchange_code_mapping.uid,
        COALESCE("OperatingMIC", exchange_code_mapping.uid)) AS "uid",
    COALESCE("OperatingMIC", exchange_code_mapping.uid) AS "mic",
    NULL AS "operating_mic",
    exchange.Name AS "name",
    exchange.Code AS "source_code",
    NULL AS "acronym",
    exchange.Currency AS "currency",
    NULL AS "city",
    exchange.CountryISO2 AS "country",
    NULL AS "website",
    NULL AS "status",
    NULL AS "comment",
    NULL AS "timezone",
    COALESCE(virtual_exchange_mapping.uid, 0) AS "is_virtual",
    "data_source" AS "source"
FROM
    $exchange AS exchange
    LEFT JOIN $exchange_code_mapping AS exchange_code_mapping ON exchange.Code = exchange_code_mapping.source_value
    LEFT JOIN $virtual_exchange_mapping AS virtual_exchange_mapping ON exchange.Code = virtual_exchange_mapping.source_value
WHERE
    exchange.Code NOT IN ('IL', 'VX')
