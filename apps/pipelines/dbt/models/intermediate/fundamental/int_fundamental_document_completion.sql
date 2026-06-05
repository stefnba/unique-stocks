{{ config(tags=['ingestion_control']) }}

WITH document AS (
    SELECT *
    FROM {{ ref('stg_fundamental_document') }}
),

final AS (
    SELECT DISTINCT
        data_provider,
        ticker AS provider_symbol,
        snapshot_date
    FROM document
)

SELECT * FROM final
