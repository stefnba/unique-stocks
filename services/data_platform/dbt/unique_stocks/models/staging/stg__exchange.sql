WITH exchange_ingestion AS (
    SELECT
        "name",
        "code",
        "operatingmic" AS "mic",
        NULLIF(REPLACE("currency", 'Unknown'), '') AS "currency",
        NULLIF(
            "countryiso2",
            ''
        ) AS "country_code",
        "ingested_at"
    FROM
        {{ source(
            'ingestion',
            'exchange'
        ) }}
    WHERE
        ingested_at = (
            SELECT
                MAX(ingested_at)
            FROM
                {{ source(
                    'ingestion',
                    'exchange'
                ) }}
        )
),
virtual_exchange AS (
    SELECT
        "source_value",
        "mapping_value"
    FROM
        {{ ref('mpg_mapping') }}
    WHERE
        "product" = 'exchange'
        AND "is_active" = TRUE
        AND "source" = 'EodHistoricalData'
        AND field = 'is_virtual'
),
composite_exchange AS (
    SELECT
        "source_value",
        "mapping_value"
    FROM
        {{ ref('mpg_mapping') }}
    WHERE
        "product" = 'exchange'
        AND "is_active" = TRUE
        AND "source" = 'EodHistoricalData'
        AND field = 'composite_code'
),
mic_code AS (
    SELECT
        "source_value",
        "mapping_value"
    FROM
        "iceberg_warehouse"."mapping"."mpg_mapping"
    WHERE
        "product" = 'exchange'
        AND "is_active" = TRUE
        AND "source" = 'EodHistoricalData'
        AND field = 'code_to_mic'
),
FINAL AS (
    SELECT
        e.name,
        COALESCE(
            ce."source_value",
            e."code"
        ) AS code,
        IF(
            ce."source_value" IS NOT NULL,
            e."code",
            NULL
        ) AS composite_code,
        COALESCE(
            mc."mapping_value",
            e."mic"
        ) AS mic,
        e.currency,
        e.country_code,
        ve."mapping_value" = '1' AS "is_virtual",
        e.ingested_at
    FROM
        exchange_ingestion e
        LEFT JOIN virtual_exchange ve
        ON e."code" = ve."source_value"
        LEFT JOIN composite_exchange ce
        ON e."code" = ce."mapping_value"
        LEFT JOIN mic_code mc
        ON COALESCE(
            ce."source_value",
            e."code"
        ) = mc."source_value"
)
SELECT
    *
FROM
    FINAL
