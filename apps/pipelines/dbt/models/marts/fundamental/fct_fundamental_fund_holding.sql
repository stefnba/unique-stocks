WITH etf_holding AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_instrument_code,
        snapshot_date,
        'etf' AS instrument_family,
        'etf_holding' AS holding_source,
        holding_provider_key,
        holding_provider_exchange_code,
        holding_provider_instrument_code,
        CAST(NULL AS BIGINT) AS provider_position,
        holding_name,
        sector,
        industry,
        country,
        region,
        assets_percent AS weight_percent,
        is_top_10,
        ingested_at
    FROM {{ ref('stg_fundamental_etf_holding') }}
),

mutual_fund_holding AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_instrument_code,
        snapshot_date,
        'fund' AS instrument_family,
        'mutual_fund_holding' AS holding_source,
        CAST(NULL AS VARCHAR) AS holding_provider_key,
        CAST(NULL AS VARCHAR) AS holding_provider_exchange_code,
        CAST(NULL AS VARCHAR) AS holding_provider_instrument_code,
        provider_position,
        holding_name,
        CAST(NULL AS VARCHAR) AS sector,
        CAST(NULL AS VARCHAR) AS industry,
        CAST(NULL AS VARCHAR) AS country,
        CAST(NULL AS VARCHAR) AS region,
        weight_percent,
        CAST(NULL AS BOOLEAN) AS is_top_10,
        ingested_at
    FROM {{ ref('stg_fundamental_mutual_fund_holding') }}
),

holding AS (
    SELECT * FROM etf_holding
    UNION ALL
    SELECT * FROM mutual_fund_holding
),

instrument AS (
    SELECT
        instrument_pk,
        data_provider,
        provider_exchange_code,
        provider_instrument_code
    FROM {{ ref('dim_instrument_core') }}
),

final AS (
    SELECT
        {{ surrogate_key([
            "holding.data_provider",
            "holding.provider_exchange_code",
            "holding.provider_instrument_code",
            "holding.snapshot_date",
            "holding.holding_source",
            "holding.holding_provider_key",
            "holding.provider_position",
            "holding.holding_name",
        ]) }} AS fundamental_fund_holding_pk,
        instrument.instrument_pk,
        holding.data_provider,
        holding.provider_exchange_code,
        holding.provider_instrument_code,
        holding.snapshot_date,
        holding.instrument_family,
        holding.holding_source,
        holding.holding_provider_key,
        holding.holding_provider_exchange_code,
        holding.holding_provider_instrument_code,
        holding.provider_position,
        holding.holding_name,
        holding.sector,
        holding.industry,
        holding.country,
        holding.region,
        holding.weight_percent,
        holding.is_top_10,
        holding.ingested_at
    FROM holding
    LEFT JOIN instrument
        ON holding.data_provider = instrument.data_provider
        AND holding.provider_exchange_code = instrument.provider_exchange_code
        AND holding.provider_instrument_code = instrument.provider_instrument_code
)

SELECT * FROM final
