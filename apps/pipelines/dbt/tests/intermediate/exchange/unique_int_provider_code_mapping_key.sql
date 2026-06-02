SELECT provider_code_mapping_id
FROM {{ ref('int_provider_code_mapping') }}
GROUP BY 1
HAVING COUNT(*) > 1
