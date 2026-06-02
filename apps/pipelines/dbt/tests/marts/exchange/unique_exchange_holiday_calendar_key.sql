SELECT exchange_holiday_calendar_id
FROM {{ ref('exchange_holiday_calendar') }}
GROUP BY 1
HAVING COUNT(*) > 1
