SELECT exchange_calendar_id
FROM {{ ref('exchange_calendar') }}
GROUP BY 1
HAVING COUNT(*) > 1
