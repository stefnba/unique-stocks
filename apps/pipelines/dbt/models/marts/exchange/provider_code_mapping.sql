SELECT
    provider_code_mapping_id,
    data_provider,
    provider_exchange_code,
    provider_code_kind,
    source_kind,
    exchange_catalog_name,
    mapping_type,
    mapping_code,
    mapping_method,
    mapping_confidence
FROM {{ ref('int_provider_code_mapping') }}
