SELECT
    *
FROM
    surrogate_keys
WHERE
    is_active
    AND product = %(product)s
