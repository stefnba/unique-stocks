WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fundamental_stock_identity') }}
),

renamed AS (
    SELECT
        CAST(source_data.ingestion_id AS VARCHAR) AS ingestion_id,
        CAST(source_data.snapshot_date AS DATE) AS snapshot_date,
        UPPER(TRIM(CAST(source_data.provider_exchange_code AS VARCHAR))) AS provider_exchange_code,
        UPPER(TRIM(CAST(source_data.provider_instrument_code AS VARCHAR))) AS provider_instrument_code,
        NULLIF(TRIM(CAST(source_data.name AS VARCHAR)), '') AS stock_name,
        NULLIF(UPPER(TRIM(CAST(source_data.primary_ticker AS VARCHAR))), '') AS primary_ticker,
        NULLIF(TRIM(CAST(source_data.provider_listing_exchange_code AS VARCHAR)), '') AS provider_listing_exchange_code,
        NULLIF(UPPER(TRIM(CAST(source_data.currency_code AS VARCHAR))), '') AS currency_code,
        NULLIF(TRIM(CAST(source_data.currency_name AS VARCHAR)), '') AS currency_name,
        NULLIF(TRIM(CAST(source_data.country_name AS VARCHAR)), '') AS country_name,
        NULLIF(UPPER(TRIM(CAST(source_data.country_iso AS VARCHAR))), '') AS country_iso,
        NULLIF(UPPER(TRIM(CAST(source_data.isin AS VARCHAR))), '') AS isin,
        NULLIF(UPPER(TRIM(CAST(source_data.cusip AS VARCHAR))), '') AS cusip,
        NULLIF(TRIM(CAST(source_data.cik AS VARCHAR)), '') AS cik,
        NULLIF(UPPER(TRIM(CAST(source_data.lei AS VARCHAR))), '') AS lei,
        NULLIF(UPPER(TRIM(CAST(source_data.open_figi AS VARCHAR))), '') AS open_figi,
        NULLIF(TRIM(CAST(source_data.employer_id_number AS VARCHAR)), '') AS employer_id_number,
        NULLIF(TRIM(CAST(source_data.fiscal_year_end AS VARCHAR)), '') AS fiscal_year_end,
        CAST(source_data.ipo_date AS DATE) AS ipo_date,
        NULLIF(TRIM(CAST(source_data.sector AS VARCHAR)), '') AS sector,
        NULLIF(TRIM(CAST(source_data.industry AS VARCHAR)), '') AS industry,
        NULLIF(TRIM(CAST(source_data.gic_sector AS VARCHAR)), '') AS gic_sector,
        NULLIF(TRIM(CAST(source_data.gic_group AS VARCHAR)), '') AS gic_group,
        NULLIF(TRIM(CAST(source_data.gic_industry AS VARCHAR)), '') AS gic_industry,
        NULLIF(TRIM(CAST(source_data.gic_sub_industry AS VARCHAR)), '') AS gic_sub_industry,
        NULLIF(TRIM(CAST(source_data.home_category AS VARCHAR)), '') AS home_category,
        CAST(source_data.is_delisted AS BOOLEAN) AS is_delisted,
        CAST(source_data.delisted_date AS DATE) AS delisted_date,
        CAST(source_data.full_time_employees AS BIGINT) AS full_time_employees,
        NULLIF(TRIM(CAST(source_data.web_url AS VARCHAR)), '') AS web_url,
        NULLIF(TRIM(CAST(source_data.logo_url AS VARCHAR)), '') AS logo_url,
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
