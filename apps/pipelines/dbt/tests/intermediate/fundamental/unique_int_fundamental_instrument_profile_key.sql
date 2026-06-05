SELECT fundamental_profile_id
FROM {{ ref('int_fundamental_instrument_profile') }}
GROUP BY 1
HAVING COUNT(*) > 1
