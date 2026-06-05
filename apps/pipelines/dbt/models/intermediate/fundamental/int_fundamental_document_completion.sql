{{ config(tags=['ingestion_control']) }}

WITH document AS (
    SELECT *
    FROM {{ ref('stg_fundamental_document') }}
),

final AS (
    SELECT DISTINCT
        data_provider,
        provider_exchange_code,
        provider_instrument_code,
        snapshot_date
    FROM document
)

SELECT * FROM final
