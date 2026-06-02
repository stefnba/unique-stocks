WITH document AS (
    SELECT *
    FROM {{ ref('int_fundamental_latest_document') }}
),

stock_identity AS (
    SELECT *
    FROM {{ ref('stg_fundamental_stock_identity') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY data_provider, ticker
        ORDER BY snapshot_date DESC, ingested_at DESC, ingestion_id DESC
    ) = 1
),

stock_shares_stats AS (
    SELECT *
    FROM {{ ref('stg_fundamental_stock_shares_stats') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY data_provider, ticker
        ORDER BY snapshot_date DESC, ingested_at DESC, ingestion_id DESC
    ) = 1
),

etf_identity AS (
    SELECT *
    FROM {{ ref('stg_fundamental_etf_identity') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY data_provider, ticker
        ORDER BY snapshot_date DESC, ingested_at DESC, ingestion_id DESC
    ) = 1
),

mutual_fund_identity AS (
    SELECT *
    FROM {{ ref('stg_fundamental_mutual_fund_identity') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY data_provider, ticker
        ORDER BY snapshot_date DESC, ingested_at DESC, ingestion_id DESC
    ) = 1
),

index_identity AS (
    SELECT *
    FROM {{ ref('stg_fundamental_index_identity') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY data_provider, ticker
        ORDER BY snapshot_date DESC, ingested_at DESC, ingestion_id DESC
    ) = 1
)

SELECT
    document.data_provider || ':' || document.ticker AS fundamental_profile_id,
    document.data_provider,
    document.provider_exchange_code,
    document.ticker,
    document.code,
    document.instrument_type,
    document.instrument_family,
    COALESCE(
        stock_identity.stock_name,
        etf_identity.etf_name,
        mutual_fund_identity.mutual_fund_name,
        index_identity.index_name,
        document.instrument_name
    ) AS security_name,
    COALESCE(
        stock_identity.primary_ticker,
        etf_identity.primary_ticker,
        mutual_fund_identity.primary_ticker,
        document.primary_ticker
    ) AS primary_ticker,
    COALESCE(
        stock_identity.provider_listing_exchange_code,
        etf_identity.provider_listing_exchange_code,
        mutual_fund_identity.provider_listing_exchange_code,
        index_identity.provider_listing_exchange_code,
        document.provider_listing_exchange_code
    ) AS provider_listing_exchange_code,
    COALESCE(
        stock_identity.currency_code,
        etf_identity.currency_code,
        mutual_fund_identity.currency_code,
        index_identity.currency_code
    ) AS currency_code,
    COALESCE(
        stock_identity.currency_name,
        etf_identity.currency_name,
        mutual_fund_identity.currency_name,
        index_identity.currency_name
    ) AS currency_name,
    COALESCE(
        stock_identity.country_name,
        etf_identity.country_name,
        mutual_fund_identity.country_name,
        index_identity.country_name
    ) AS country_name,
    COALESCE(
        stock_identity.country_iso,
        etf_identity.country_iso,
        mutual_fund_identity.country_iso,
        index_identity.country_iso
    ) AS country_iso,
    COALESCE(stock_identity.isin, etf_identity.isin, mutual_fund_identity.isin) AS isin,
    COALESCE(stock_identity.cusip, mutual_fund_identity.cusip) AS cusip,
    stock_identity.cik,
    stock_identity.lei,
    COALESCE(stock_identity.open_figi, etf_identity.open_figi, mutual_fund_identity.open_figi, index_identity.open_figi)
        AS open_figi,
    stock_identity.employer_id_number,
    COALESCE(stock_identity.fiscal_year_end, mutual_fund_identity.fiscal_year_end) AS fiscal_year_end,
    stock_identity.ipo_date,
    stock_identity.sector,
    stock_identity.industry,
    stock_identity.gic_sector,
    stock_identity.gic_group,
    stock_identity.gic_industry,
    stock_identity.gic_sub_industry,
    stock_identity.home_category,
    COALESCE(stock_identity.is_delisted, FALSE) AS is_delisted,
    stock_identity.delisted_date,
    stock_identity.full_time_employees,
    stock_identity.web_url,
    stock_identity.logo_url,
    stock_shares_stats.shares_outstanding,
    stock_shares_stats.shares_float,
    stock_shares_stats.percent_insiders,
    stock_shares_stats.percent_institutions,
    stock_shares_stats.short_ratio,
    stock_shares_stats.short_percent_outstanding,
    stock_shares_stats.short_percent_float,
    COALESCE(etf_identity.domicile, mutual_fund_identity.domicile) AS domicile,
    COALESCE(etf_identity.index_name, index_identity.index_name) AS tracked_index_name,
    etf_identity.company_name AS fund_company_name,
    etf_identity.company_url AS fund_company_url,
    etf_identity.etf_url,
    etf_identity.dividend_paying_frequency,
    etf_identity.holdings_count,
    mutual_fund_identity.fund_category,
    mutual_fund_identity.fund_family,
    mutual_fund_identity.fund_style,
    COALESCE(etf_identity.inception_date, mutual_fund_identity.inception_date) AS fund_inception_date,
    mutual_fund_identity.update_date AS fund_update_date,
    index_identity.market_cap AS index_market_cap,
    document.provider_updated_at,
    document.latest_snapshot_date,
    document.payload_hash,
    document.row_hash AS document_row_hash,
    document.source_uri AS document_source_uri,
    document.ingested_at AS document_ingested_at
FROM document
LEFT JOIN stock_identity
    ON document.data_provider = stock_identity.data_provider
    AND document.ticker = stock_identity.ticker
LEFT JOIN stock_shares_stats
    ON document.data_provider = stock_shares_stats.data_provider
    AND document.ticker = stock_shares_stats.ticker
LEFT JOIN etf_identity
    ON document.data_provider = etf_identity.data_provider
    AND document.ticker = etf_identity.ticker
LEFT JOIN mutual_fund_identity
    ON document.data_provider = mutual_fund_identity.data_provider
    AND document.ticker = mutual_fund_identity.ticker
LEFT JOIN index_identity
    ON document.data_provider = index_identity.data_provider
    AND document.ticker = index_identity.ticker
