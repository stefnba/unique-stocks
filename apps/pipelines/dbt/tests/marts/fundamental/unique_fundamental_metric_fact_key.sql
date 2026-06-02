SELECT fundamental_metric_fact_id
FROM {{ ref('fundamental_metric_fact') }}
GROUP BY 1
HAVING COUNT(*) > 1
