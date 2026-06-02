SELECT 'eodhd INDX namespace is missing or disabled' AS failure_reason
WHERE NOT EXISTS (
        SELECT 1
        FROM {{ ref('int_exchange_provider_ingestion_universe') }}
        WHERE data_provider = 'eodhd'
            AND provider_exchange_code = 'INDX'
            AND provider_code_kind = 'index_namespace'
            AND source_kind = 'curated_seed'
            AND is_enabled_for_instrument
            AND is_enabled_for_eod_price
            AND is_enabled_for_fundamental
            AND is_enabled_for_ingestion
    )
