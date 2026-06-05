WITH document AS (
    SELECT *
    FROM {{ ref('stg_fundamental_document') }}
)

SELECT
    data_provider || ':' || provider_exchange_code || ':' || provider_instrument_code AS latest_fundamental_document_id,
    snapshot_date AS latest_snapshot_date,
    data_provider,
    provider_exchange_code,
    provider_instrument_code,
    instrument_name,
    instrument_type,
    instrument_family,
    primary_ticker,
    provider_listing_exchange_code,
    provider_updated_at,
    top_level_sections,
    payload_hash,
    row_hash,
    source_uri,
    ingested_at
FROM document
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY data_provider, provider_exchange_code, provider_instrument_code
        ORDER BY snapshot_date DESC, provider_updated_at DESC NULLS LAST, ingested_at DESC, ingestion_id DESC
    ) = 1
