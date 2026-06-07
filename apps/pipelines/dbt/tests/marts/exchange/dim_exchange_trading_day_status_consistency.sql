SELECT *
FROM {{ ref('dim_exchange_trading_day') }}
WHERE (
    is_trading_day
    AND trading_day_status NOT IN ('open', 'early_close')
)
OR (
    NOT is_calendar_known
    AND trading_day_status != 'unknown_calendar'
)
OR (
    is_calendar_known
    AND NOT is_trading_day
    AND NOT is_closed
)
OR (
    is_trading_day
    AND is_closed
)
OR (
    is_trading_day
    AND effective_session_open IS NULL
)
OR (
    is_trading_day
    AND effective_session_close IS NULL
)
