SELECT
    mic,
    operating_mic
FROM (
    SELECT
        id,
        operating_exchange_id,
        mic
    FROM
        "data"."exchange"
    WHERE
        operating_exchange_id IS NOT NULL) AS mic
    LEFT JOIN (
        SELECT
            id,
            mic AS operating_mic
        FROM
            "data"."exchange"
        WHERE
            operating_exchange_id IS NULL) AS operating_mic ON mic.operating_exchange_id = operating_mic.id
