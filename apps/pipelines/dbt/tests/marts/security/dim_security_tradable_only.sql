SELECT security_id
FROM {{ ref('dim_security') }}
WHERE instrument_family NOT IN ('stock', 'etf', 'fund', 'bond', 'crypto', 'forex')
