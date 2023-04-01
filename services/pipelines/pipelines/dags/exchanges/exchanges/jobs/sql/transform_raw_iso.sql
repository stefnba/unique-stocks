SELECT
    "exchanges"."MIC" AS "exchange_uid",
    "exchanges"."MIC" AS "mic",
    "exchanges"."OPERATING MIC" AS "operating_mic",
    "exchanges"."LEGAL ENTITY NAME" AS "exchange_name",
    "exchanges"."ACRONYM" AS "acronym",
    NULL AS "currency",
    "exchanges"."CITY" AS "city",
    "exchanges"."ISO COUNTRY CODE (ISO 3166)" AS "country",
    "exchanges"."WEBSITE" AS "website",
    $source AS "data_source",
    "exchanges"."CREATION DATE" AS "created_at",
    "exchanges"."LAST UPDATE DATE" AS "updated_at",
    "exchanges"."COMMENTS" AS "comments",
    "exchanges"."LEI" AS "legal_entity_identifier",
    strptime ("exchanges"."LAST VALIDATION DATE", '%Y%m%d') AS "validated_at",
    "exchanges"."EXPIRY DATE" AS "expires_at",
    "exchanges"."MARKET CATEGORY CODE" AS "market_category_code",
    "exchanges"."MARKET NAME-INSTITUTION DESCRIPTION" AS "market_name_institution"
FROM
    $exchanges AS exchanges
WHERE
    "OPRT/SGMT" = 'OPRT'
    AND "STATUS" = 'ACTIVE'
