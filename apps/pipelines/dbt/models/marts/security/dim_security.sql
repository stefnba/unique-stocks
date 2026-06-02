WITH instrument AS (
    SELECT *
    FROM {{ ref('int_latest_instrument_universe') }}
    WHERE is_tradable
),

fundamentals AS (
    SELECT *
    FROM {{ ref('int_fundamental_security_profile') }}
)

SELECT
    instrument.data_provider || ':' || instrument.provider_exchange_code || ':' || instrument.ticker AS security_id,
    instrument.data_provider,
    instrument.provider_exchange_code,
    instrument.provider_code_kind,
    instrument.provider_code_source_kind,
    instrument.exchange_catalog_name,
    instrument.exchange_country_iso2,
    instrument.exchange_currency,
    instrument.ticker,
    instrument.provider_symbol,
    COALESCE(fundamentals.security_name, instrument.instrument_name) AS security_name,
    fundamentals.primary_ticker,
    COALESCE(fundamentals.currency_code, instrument.currency) AS currency_code,
    COALESCE(fundamentals.country_name, instrument.country) AS country_name,
    fundamentals.country_iso,
    instrument.asset_type,
    instrument.normalized_asset_type,
    instrument.instrument_family,
    instrument.is_stock,
    instrument.is_fund_like,
    instrument.has_isin,
    COALESCE(fundamentals.isin, instrument.isin) AS isin,
    fundamentals.cusip,
    fundamentals.cik,
    fundamentals.lei,
    fundamentals.open_figi,
    fundamentals.sector,
    fundamentals.industry,
    fundamentals.gic_sector,
    fundamentals.gic_group,
    fundamentals.gic_industry,
    fundamentals.gic_sub_industry,
    COALESCE(fundamentals.is_delisted, FALSE) AS is_delisted,
    fundamentals.delisted_date,
    fundamentals.shares_outstanding,
    fundamentals.shares_float,
    fundamentals.domicile,
    fundamentals.tracked_index_name,
    fundamentals.holdings_count,
    fundamentals.fund_category,
    fundamentals.fund_family,
    fundamentals.fund_style,
    fundamentals.fund_inception_date,
    fundamentals.provider_updated_at AS fundamentals_provider_updated_at,
    fundamentals.latest_snapshot_date AS fundamentals_snapshot_date,
    fundamentals.fundamental_profile_id IS NOT NULL AS has_fundamentals,
    instrument.snapshot_date AS instrument_snapshot_date,
    instrument.row_hash AS instrument_row_hash,
    instrument.source_uri AS instrument_source_uri,
    instrument.ingested_at AS instrument_ingested_at
FROM instrument
LEFT JOIN fundamentals
    ON instrument.data_provider = fundamentals.data_provider
    AND instrument.provider_symbol = fundamentals.ticker
