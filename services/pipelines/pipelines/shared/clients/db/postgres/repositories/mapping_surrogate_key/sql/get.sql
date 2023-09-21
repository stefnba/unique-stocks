SELECT
    surrogate_key,
    uid
FROM
    mapping.surrogate_key
WHERE
    is_active
    AND product = %(product)s
