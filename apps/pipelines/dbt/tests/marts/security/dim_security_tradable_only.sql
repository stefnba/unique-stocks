SELECT security_pk
FROM {{ ref('dim_security') }}
WHERE instrument_family NOT IN ('stock', 'etf', 'fund', 'bond', 'crypto', 'forex')
