SELECT
    "securities"."Code" AS "source_code",
    "securities"."ISIN" AS "isin",
    "securities"."Name" AS "name",
    "securities"."Type" AS "source_type",
    "security_type_mapping"."uid" AS "type_uid",
    "country_mapping"."uid" AS "country",
    "securities"."Currency" AS "currency",
    "securities"."Exchange" AS "exchange_source_code",
    "exchange_mapping"."uid" AS "exchange_uid",
    $source AS "source"
FROM
    $securities AS "securities"
    LEFT JOIN $security_type_mapping AS "security_type_mapping" ON "security_type_mapping"."source_value" = "securities"."Type"
    LEFT JOIN $country_mapping AS "country_mapping" ON "country_mapping"."source_value" = "securities"."Country"
    LEFT JOIN $exchange_mapping AS "exchange_mapping" ON "exchange_mapping"."source_value" = "securities"."Exchange"
