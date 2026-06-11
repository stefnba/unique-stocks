WITH instrument AS (
    SELECT *
    FROM {{ ref('dim_instrument_core') }}
),

fundamentals AS (
    SELECT *
    FROM {{ ref('int_fundamental_instrument_profile') }}
),

final AS (
    SELECT
        {{ surrogate_key([
            "instrument.data_provider",
            "instrument.provider_exchange_code",
            "instrument.provider_instrument_code",
            ]) }}
            AS instrument_pk,
        instrument.data_provider,
        instrument.provider_exchange_code,
        instrument.provider_code_kind,
        instrument.provider_code_source_kind,
        instrument.exchange_catalog_name,
        instrument.exchange_country_iso2,
        instrument.exchange_currency,
        instrument.provider_instrument_code,
        COALESCE(fundamentals.instrument_name, instrument.instrument_name) AS instrument_name,
        fundamentals.primary_ticker,
        COALESCE(fundamentals.currency_code, instrument.currency_code) AS currency_code,
        COALESCE(fundamentals.country_name, instrument.country_name) AS country_name,
        fundamentals.country_iso,
        instrument.asset_type,
        instrument.normalized_asset_type,
        instrument.instrument_family,
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
        fundamentals.domicile,
        fundamentals.tracked_index_name,
        fundamentals.fund_category,
        fundamentals.fund_family,
        fundamentals.fund_style,
        instrument.is_stock,
        instrument.is_fund_like,
        instrument.has_isin,
        COALESCE(fundamentals.is_delisted, FALSE) AS is_delisted,
        fundamentals.fundamental_profile_id IS NOT NULL AS has_fundamentals,
        fundamentals.shares_outstanding,
        fundamentals.shares_float,
        fundamentals.holdings_count,
        fundamentals.delisted_date,
        fundamentals.fund_inception_date,
        fundamentals.provider_updated_at AS fundamentals_provider_updated_at,
        fundamentals.latest_snapshot_date AS fundamentals_snapshot_date,
        instrument.instrument_snapshot_date,
        instrument.instrument_ingested_at
    FROM instrument
    LEFT JOIN fundamentals
        ON instrument.data_provider = fundamentals.data_provider
        AND instrument.provider_exchange_code = fundamentals.provider_exchange_code
        AND instrument.provider_instrument_code = fundamentals.provider_instrument_code
)

SELECT * FROM final
