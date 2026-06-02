SELECT instrument_universe_id
FROM {{ ref('int_latest_instrument_universe') }}
GROUP BY 1
HAVING COUNT(*) > 1
