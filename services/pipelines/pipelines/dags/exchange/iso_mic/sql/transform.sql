SELECT
    "exchanges"."MIC" AS "uid",
    "exchanges"."MIC" AS "mic",
    "exchanges"."OPERATING MIC" AS "operating_mic",
    "exchanges"."MARKET NAME-INSTITUTION DESCRIPTION" AS "name",
    "exchanges"."LEGAL ENTITY NAME" AS "legal_entity_name",
    NULL AS "source_code",
    "exchanges"."ACRONYM" AS "acronym",
    NULL AS "currency",
    "exchanges"."CITY" AS "city",
    "exchanges"."ISO COUNTRY CODE (ISO 3166)" AS "country",
    "exchanges"."WEBSITE" AS "website",
    "STATUS" AS "status",
    --"exchanges"."CREATION DATE" AS "record_created_at",
    --"exchanges"."LAST UPDATE DATE" AS "record_updated_at",
    "exchanges"."COMMENTS" AS "comment",
    NULL AS "timezone",
    "exchanges"."LEI" AS "legal_entity_identifier",
    strptime("exchanges"."LAST VALIDATION DATE", '%Y%m%d') AS "validated_at",
    "exchanges"."EXPIRY DATE" AS "expires_at",
    "exchanges"."MARKET CATEGORY CODE" AS "market_category_code",
    0 AS "is_virtual",
    $source AS "source"
FROM
    $exchanges AS exchanges
WHERE "exchanges"."MIC" <> 'INDX'
