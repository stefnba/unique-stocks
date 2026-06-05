SELECT
    data_provider,
    provider_exchange_code,
    provider_instrument_code,
    snapshot_date
FROM {{ ref('int_fundamental_document_completion') }}
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
