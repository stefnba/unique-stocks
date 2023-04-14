SELECT
    "exchange"."MIC" AS "exchange_uid",
    "exchange"."MIC" AS "mic",
    "exchange"."OPERATING MIC" AS "operating_mic",
    "exchange"."LEGAL ENTITY NAME" AS "exchange_name",
    "exchange"."ACRONYM" AS "acronym",
    NULL AS "currency",
    "exchange"."CITY" AS "city",
    "exchange"."ISO COUNTRY CODE (ISO 3166)" AS "country",
    "exchange"."WEBSITE" AS "website",
    $source AS "data_source",
    "exchange"."CREATION DATE" AS "created_at",
    "exchange"."LAST UPDATE DATE" AS "updated_at",
    "exchange"."COMMENTS" AS "comments",
    "exchange"."LEI" AS "legal_entity_identifier",
    strptime("exchange"."LAST VALIDATION DATE", '%Y%m%d') AS "validated_at",
    "exchange"."EXPIRY DATE" AS "expires_at",
    "exchange"."MARKET CATEGORY CODE" AS "market_category_code",
    "exchange"."MARKET NAME-INSTITUTION DESCRIPTION" AS "market_name_institution"
FROM
    $exchange AS exchange
WHERE
    "OPRT/SGMT" = 'OPRT'
    AND "STATUS" = 'ACTIVE'
