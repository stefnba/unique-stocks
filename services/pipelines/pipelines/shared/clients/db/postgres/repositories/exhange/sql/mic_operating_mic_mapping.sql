SELECT
    mic,
    operating_mic
FROM (
    SELECT
        id,
        operating_exchange_id,
        mic
    FROM
        exchange
    WHERE
        operating_exchange_id IS NOT NULL) AS mic
    LEFT JOIN (
        SELECT
            id,
            mic AS operating_mic
        FROM
            exchange
        WHERE
            operating_exchange_id IS NULL) AS operating_mic ON mic.operating_exchange_id = operating_mic.id
