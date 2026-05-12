WITH exchange_security_ingestion AS (
    SELECT
        code,
        NAME,
        CASE
            WHEN exchange = 'MU'
            AND exchange_code = 'DU' THEN exchange_code
            WHEN exchange = 'XHEL'
            AND exchange_code = 'HE' THEN exchange_code
            WHEN exchange = 'NSE'
            AND exchange_code = 'EUFUND' THEN exchange_code
            ELSE exchange
        END AS exchange_code,
        currency,
        TYPE,
        isin
    FROM
        {{ source(
            'ingestion',
            'exchange_security'
        ) }}
    WHERE
        ingested_at = (
            SELECT
                MAX(ingested_at)
            FROM
                {{ source(
                    'ingestion',
                    'exchange_security'
                ) }}
        )
),
security_type_mapping AS (
    SELECT
        "source_value",
        "mapping_value"
    FROM
        {{ ref('mpg_mapping') }}
    WHERE
        "product" = 'security'
        AND "is_active" = TRUE
        AND "source" = 'EodHistoricalData'
        AND field = 'type'
),
FINAL AS (
    SELECT
        es.code,
        es.name,
        es.exchange_code,
        es.currency,
        es.isin,
        stm.mapping_value AS "type"
    FROM
        exchange_security_ingestion es
        LEFT JOIN security_type_mapping stm
        ON es.type = stm.source_value
)
SELECT
    *
FROM
    FINAL
