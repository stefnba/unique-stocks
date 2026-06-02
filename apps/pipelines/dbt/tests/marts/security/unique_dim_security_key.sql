SELECT security_id
FROM {{ ref('dim_security') }}
GROUP BY 1
HAVING COUNT(*) > 1
