{{ config(
    unique_key = ['exchange_code', 'security_code', 'date'],
    properties ={ "partitioning": "ARRAY['year(date)']",}
) }}

WITH exchange AS (

    SELECT
        *
    FROM
        {{ ref('stg__security_quote') }}
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
