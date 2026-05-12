{{ config(
    unique_key = ['code', 'exchange_code'],
) }}

WITH exchange_security AS (

    SELECT
        *
    FROM
        {{ ref('stg__exchange_security') }}
),
FINAL AS (
    SELECT
        *
    FROM
        exchange_security
)
SELECT
    *
FROM
    FINAL
