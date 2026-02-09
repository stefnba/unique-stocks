WITH security_quote_ingestion AS (
    SELECT
        DATE,
        OPEN,
        high,
        low,
        CLOSE,
        adjusted_close,
        CAST(
            volume AS bigint
        ) AS volume,
        ingested_at,
        exchange_code,
        security_code
    FROM
        {{ source(
            'ingestion',
            'security_quote'
        ) }}
    WHERE
        security_code <> 'AAPL'
        AND ingested_at = (
            SELECT
                MAX(ingested_at)
            FROM
                {{ source(
                    'ingestion',
                    'security_quote'
                ) }}
        )
),
FINAL AS (
    SELECT
        *
    FROM
        security_quote_ingestion
)
SELECT
    *
FROM
    FINAL
