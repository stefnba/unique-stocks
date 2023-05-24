SELECT
    COALESCE("exchange_code_mapping"."uid", "exchange"."mic") AS "uid",
    COALESCE(exchange_code_mapping.uid, exchange.mic) AS "mic",
    NULL AS "operating_mic",
    exchange.name AS "name",
    exchange.mic AS "source_code",
    exchange.acronym,
    STRUCT_EXTRACT(exchange.currency, 'code') AS currency,
    exchange.city,
    IF (virtual_exchange_mapping.uid IS NOT NULL,
        NULL,
        country_code) AS country,
    IF (LENGTH(website) == 0,
        NULL,
        website) AS website,
    NULL AS "status",
    NULL AS "comment",
    STRUCT_EXTRACT(exchange.timezone, 'timezone') AS "timezone",
    COALESCE(virtual_exchange_mapping.uid, 0) AS "is_virtual",
    exchange.data_source AS "source"
FROM
    $exchange AS exchange
    LEFT JOIN $exchange_code_mapping AS exchange_code_mapping ON exchange_code_mapping.source_value = exchange.mic
    LEFT JOIN $virtual_exchange_mapping AS virtual_exchange_mapping ON virtual_exchange_mapping.source_value = exchange.mic
