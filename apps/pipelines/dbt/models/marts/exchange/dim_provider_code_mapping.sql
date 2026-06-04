WITH provider_code_mapping AS (
    SELECT *
    FROM {{ ref('int_provider_code_mapping') }}
),

final AS (
    SELECT
        {{ surrogate_key(["data_provider", "provider_exchange_code", "mapping_type", "mapping_code"]) }}
            AS provider_code_mapping_pk,
        data_provider,
        provider_exchange_code,
        provider_code_kind,
        source_kind,
        exchange_catalog_name,
        mapping_type,
        mapping_code,
        mapping_method,
        mapping_confidence
    FROM provider_code_mapping
)

SELECT * FROM final
