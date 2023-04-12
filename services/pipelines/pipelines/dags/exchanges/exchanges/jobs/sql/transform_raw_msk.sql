SELECT
    COALESCE("exchange_code_mappings"."uid", "exchanges"."mic") AS "exchange_uid",
    COALESCE(exchange_code_mappings.uid, exchanges.mic) AS mic,
    exchanges.mic AS exchange_source_code,
    exchanges.name AS exchange_name,
    exchanges.acronym,
    STRUCT_EXTRACT (exchanges.currency, 'code') AS currency,
    exchanges.city,
    IF (virtual_exchange_mapping.uid IS NOT NULL,
        NULL,
        country_code) AS country,
    IF (LENGTH(website) == 0,
        NULL,
        website) AS website,
    COALESCE(virtual_exchange_mapping.uid, 0) AS is_virtual,
    STRUCT_EXTRACT (exchanges.timezone, 'timezone') AS timezone,
    exchanges.data_source,
FROM
    $exchanges AS exchanges
    LEFT JOIN $exchange_code_mappings AS exchange_code_mappings ON exchange_code_mappings.source_value = exchanges.mic
    LEFT JOIN $virtual_exchange_mapping AS virtual_exchange_mapping ON virtual_exchange_mapping.source_value = exchanges.mic
