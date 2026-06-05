WITH staged_instrument AS (
    SELECT *
    FROM {{ ref('stg_instrument') }}
),

latest_snapshot AS (
    SELECT
        data_provider,
        provider_exchange_code,
        MAX(snapshot_date) AS latest_snapshot_date
    FROM staged_instrument
    GROUP BY 1, 2
),

classified_instrument AS (
    SELECT
        *,
        CASE
            WHEN normalized_asset_type LIKE '%etf%' THEN 'etf'
            WHEN normalized_asset_type LIKE '%fund%' THEN 'fund'
            WHEN normalized_asset_type LIKE '%stock%' THEN 'stock'
            WHEN normalized_asset_type LIKE '%bond%' THEN 'bond'
            WHEN normalized_asset_type LIKE '%index%' THEN 'index'
            WHEN provider_exchange_code = 'CC' THEN 'crypto'
            WHEN provider_exchange_code = 'FOREX' THEN 'forex'
            ELSE 'unknown'
        END AS instrument_family,
        isin IS NOT NULL AS has_isin
    FROM staged_instrument
),

instrument AS (
    SELECT
        *,
        instrument_family IN ('stock', 'etf', 'fund', 'bond', 'crypto', 'forex') AS is_tradable,
        instrument_family = 'stock' AS is_stock,
        instrument_family IN ('etf', 'fund') AS is_fund_like,
        instrument_family = 'index' AS is_index
    FROM classified_instrument
),

provider_universe AS (
    SELECT *
    FROM {{ ref('int_exchange_provider_ingestion_universe') }}
)

SELECT
    instrument.data_provider
    || ':'
    || instrument.provider_exchange_code
    || ':'
    || instrument.provider_instrument_code AS instrument_universe_id,
    instrument.snapshot_date,
    instrument.data_provider,
    instrument.provider_exchange_code,
    provider_universe.provider_code_kind,
    provider_universe.source_kind AS provider_code_source_kind,
    provider_universe.exchange_catalog_name,
    provider_universe.country_iso2 AS exchange_country_iso2,
    provider_universe.currency AS exchange_currency,
    instrument.provider_instrument_code,
    instrument.instrument_name,
    instrument.country,
    instrument.provider_listing_exchange_code,
    instrument.currency,
    instrument.asset_type,
    instrument.normalized_asset_type,
    instrument.instrument_family,
    instrument.is_tradable,
    instrument.is_stock,
    instrument.is_fund_like,
    instrument.is_index,
    instrument.isin,
    instrument.has_isin,
    TRUE AS is_active_in_provider_universe,
    instrument.row_hash,
    instrument.source_uri,
    instrument.ingested_at
FROM instrument
INNER JOIN latest_snapshot
    ON instrument.data_provider = latest_snapshot.data_provider
    AND instrument.provider_exchange_code = latest_snapshot.provider_exchange_code
    AND instrument.snapshot_date = latest_snapshot.latest_snapshot_date
LEFT JOIN provider_universe
    ON instrument.data_provider = provider_universe.data_provider
    AND instrument.provider_exchange_code = provider_universe.provider_exchange_code
