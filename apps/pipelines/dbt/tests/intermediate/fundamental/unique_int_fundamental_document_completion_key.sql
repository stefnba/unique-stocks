SELECT
    data_provider,
    provider_symbol,
    snapshot_date
FROM {{ ref('int_fundamental_document_completion') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
