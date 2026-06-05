WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_mutual_fund_identity') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.provider_instrument_code AS VARCHAR))) AS provider_instrument_code,
        NULLIF(TRIM(CAST(source_data.name AS VARCHAR)), '') AS mutual_fund_name,
        NULLIF(UPPER(TRIM(CAST(source_data.primary_ticker AS VARCHAR))), '') AS primary_ticker,
        NULLIF(TRIM(CAST(source_data.provider_listing_exchange_code AS VARCHAR)), '') AS provider_listing_exchange_code,
        NULLIF(UPPER(TRIM(CAST(source_data.currency_code AS VARCHAR))), '') AS currency_code,
        NULLIF(TRIM(CAST(source_data.currency_name AS VARCHAR)), '') AS currency_name,
        NULLIF(TRIM(CAST(source_data.country_name AS VARCHAR)), '') AS country_name,
        NULLIF(UPPER(TRIM(CAST(source_data.country_iso AS VARCHAR))), '') AS country_iso,
        NULLIF(UPPER(TRIM(CAST(source_data.isin AS VARCHAR))), '') AS isin,
        NULLIF(UPPER(TRIM(CAST(source_data.cusip AS VARCHAR))), '') AS cusip,
        NULLIF(UPPER(TRIM(CAST(source_data.open_figi AS VARCHAR))), '') AS open_figi,
        NULLIF(TRIM(CAST(source_data.fund_category AS VARCHAR)), '') AS fund_category,
        NULLIF(TRIM(CAST(source_data.fund_family AS VARCHAR)), '') AS fund_family,
        NULLIF(TRIM(CAST(source_data.fund_style AS VARCHAR)), '') AS fund_style,
        NULLIF(TRIM(CAST(source_data.fiscal_year_end AS VARCHAR)), '') AS fiscal_year_end,
        NULLIF(TRIM(CAST(source_data.domicile AS VARCHAR)), '') AS domicile,
        CAST(source_data.inception_date AS DATE) AS inception_date,
        CAST(source_data.update_date AS DATE) AS update_date,
        LOWER(TRIM(CAST(source_data.data_provider AS VARCHAR))) AS data_provider,
        CAST(source_data.row_hash AS VARCHAR) AS row_hash,
        CAST(source_data.source_uri AS VARCHAR) AS source_uri,
        CAST(source_data.ingested_at AS TIMESTAMPTZ) AS ingested_at
    FROM source AS source_data
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_date, provider_exchange_code, provider_instrument_code, data_provider
            ORDER BY ingested_at DESC, ingestion_id DESC
        ) AS row_number
    FROM renamed
)

SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1
