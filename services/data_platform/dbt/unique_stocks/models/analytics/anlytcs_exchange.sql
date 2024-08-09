{{ config(
    unique_key = 'code',
) }}

WITH exchange AS (

    SELECT
        *
    FROM
        {{ ref('stg_exchange') }}
),
FINAL AS (
    SELECT
        *
    FROM
        exchange
)
SELECT
    *
FROM
    FINAL
