WITH instrument AS (
    SELECT *
    FROM {{ ref('int_latest_instrument_universe') }}
    WHERE is_tradable
),

final AS (
    SELECT
        {{ surrogate_key([
            "data_provider",
            "provider_exchange_code",
            "provider_instrument_code",
        ]) }}
            AS instrument_pk,
        data_provider,
        provider_exchange_code,
        provider_code_kind,
        provider_code_source_kind,
        exchange_catalog_name,
        exchange_country_iso2,
        exchange_currency,
        provider_instrument_code,
        instrument_name,
        provider_listing_exchange_code,
        currency AS currency_code,
        country AS country_name,
        asset_type,
        normalized_asset_type,
        instrument_family,
        isin,
        is_stock,
        is_fund_like,
        has_isin,
        snapshot_date AS instrument_snapshot_date,
        ingested_at AS instrument_ingested_at
    FROM instrument
)

SELECT * FROM final
