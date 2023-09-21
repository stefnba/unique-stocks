SELECT
    "exchange"."id",
    "exchange"."mic",
    "exchange"."name",
    "exchange"."is_virtual",
    "exchange"."website",
    "exchange"."acronym",
    "exchange"."source",
    "operating_exchange"."operating_exchange"
FROM
    "data"."exchange" AS "exchange"
    LEFT JOIN LATERAL (
        SELECT
            json_build_object('id', "operating_exchange"."id", 'mic', "operating_exchange"."mic", 'name', "operating_exchange"."name") AS "operating_exchange"
        FROM
            "data"."exchange" AS "operating_exchange"
        WHERE
            "exchange"."operating_exchange_id" = "operating_exchange"."id") "operating_exchange" ON TRUE
