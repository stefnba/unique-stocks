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
OR (
    is_trading_day
    AND effective_session_open_local_at IS NULL
)
OR (
    is_trading_day
    AND effective_session_close_local_at IS NULL
)
OR (
    is_trading_day
    AND effective_session_open_utc_at IS NULL
)
OR (
    is_trading_day
    AND effective_session_close_utc_at IS NULL
)
OR (
    is_trading_day
    AND utc_offset_minutes_at_session_open IS NULL
)
OR (
    is_trading_day
    AND utc_offset_minutes_at_session_close IS NULL
)
OR (
    effective_session_open_utc_at IS NOT NULL
    AND utc_offset_minutes_at_session_open != DATE_DIFF(
        'minute',
        effective_session_open_utc_at,
        effective_session_open_local_at
    )
)
OR (
    effective_session_close_utc_at IS NOT NULL
    AND utc_offset_minutes_at_session_close != DATE_DIFF(
        'minute',
        effective_session_close_utc_at,
        effective_session_close_local_at
    )
)
