WITH current_component AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_instrument_code,
        snapshot_date,
        'current_component' AS component_source,
        provider_position,
        component_provider_exchange_code,
        component_provider_instrument_code,
        component_name,
        sector,
        industry,
        weight,
        CAST(NULL AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        CAST(TRUE AS BOOLEAN) AS is_active_now,
        CAST(NULL AS BOOLEAN) AS is_delisted,
        ingested_at
    FROM {{ ref('stg_fundamental_index_component') }}
),

historical_component AS (
    SELECT
        data_provider,
        provider_exchange_code,
        provider_instrument_code,
        snapshot_date,
        'historical_component' AS component_source,
        provider_position,
        CAST(NULL AS VARCHAR) AS component_provider_exchange_code,
        component_provider_instrument_code,
        component_name,
        CAST(NULL AS VARCHAR) AS sector,
        CAST(NULL AS VARCHAR) AS industry,
        CAST(NULL AS DECIMAL(38, 10)) AS weight,
        start_date,
        end_date,
        is_active_now,
        is_delisted,
        ingested_at
    FROM {{ ref('stg_fundamental_index_historical_component') }}
),

component AS (
    SELECT * FROM current_component
    UNION ALL
    SELECT * FROM historical_component
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
            "component.data_provider",
            "component.provider_exchange_code",
            "component.provider_instrument_code",
            "component.snapshot_date",
            "component.component_source",
            "component.provider_position",
            "component.component_provider_instrument_code",
            "component.start_date",
            "component.end_date",
        ]) }} AS fundamental_index_component_pk,
        instrument.instrument_pk,
        component.data_provider,
        component.provider_exchange_code,
        component.provider_instrument_code,
        component.snapshot_date,
        component.component_source,
        component.provider_position,
        component.component_provider_exchange_code,
        component.component_provider_instrument_code,
        component.component_name,
        component.sector,
        component.industry,
        component.weight,
        component.start_date,
        component.end_date,
        component.is_active_now,
        component.is_delisted,
        component.ingested_at
    FROM component
    LEFT JOIN instrument
        ON component.data_provider = instrument.data_provider
        AND component.provider_exchange_code = instrument.provider_exchange_code
        AND component.provider_instrument_code = instrument.provider_instrument_code
)

SELECT * FROM final
