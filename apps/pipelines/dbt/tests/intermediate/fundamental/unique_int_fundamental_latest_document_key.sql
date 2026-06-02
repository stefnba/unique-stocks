SELECT latest_fundamental_document_id
FROM {{ ref('int_fundamental_latest_document') }}
GROUP BY 1
HAVING COUNT(*) > 1
