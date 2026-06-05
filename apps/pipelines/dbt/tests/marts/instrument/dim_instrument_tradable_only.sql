SELECT instrument_pk
FROM {{ ref('dim_instrument') }}
WHERE instrument_family NOT IN ('stock', 'etf', 'fund', 'bond', 'crypto', 'forex')
