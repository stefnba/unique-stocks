SELECT fundamental_profile_id
FROM {{ ref('security_fundamental_profile') }}
GROUP BY 1
HAVING COUNT(*) > 1
