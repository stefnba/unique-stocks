SELECT
    security.name,
    entity_isin.isin,
    security.figi,
    security.security_type_id
FROM
    data.entity_isin
    LEFT JOIN data.security ON entity_isin.id = security.entity_isin_id
WHERE
    entity_isin.entity_id = 46177645
    AND "security".id IS NOT NULL
