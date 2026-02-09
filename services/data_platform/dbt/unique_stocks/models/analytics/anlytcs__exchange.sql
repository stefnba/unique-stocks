{{ config(
    unique_key = 'code',
) }}

WITH exchange AS (

    SELECT
        *
    FROM
        {{ ref('stg__exchange') }}
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
