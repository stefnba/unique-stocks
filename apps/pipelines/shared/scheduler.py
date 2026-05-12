"""
NYSE trading calendar utilities.

Uses exchange_calendars (formerly trading_calendars) under the hood.
The NYSE calendar is the default; US equities are what we ingest.
"""

from datetime import date, timedelta
from functools import lru_cache

import exchange_calendars as xcals


@lru_cache(maxsize=1)
def _nyse() -> xcals.ExchangeCalendar:
    return xcals.get_calendar("XNYS")


def is_trading_day(d: date) -> bool:
    """Return True if the NYSE was open on date d."""
    cal = _nyse()
    # exchange_calendars expects a Timestamp-compatible string
    return bool(cal.is_session(d.isoformat()))


def trading_days_between(start: date, end: date) -> list[date]:
    """Return all NYSE trading days in [start, end], inclusive."""
    cal = _nyse()
    sessions = cal.sessions_in_range(start.isoformat(), end.isoformat())
    return [s.date() for s in sessions]


def previous_trading_day(d: date) -> date:
    """Return the most recent trading day strictly before d."""
    cal = _nyse()
    prev = cal.previous_session(d.isoformat())
    return prev.date()


def next_trading_day(d: date) -> date:
    """Return the next trading day strictly after d."""
    cal = _nyse()
    nxt = cal.next_session(d.isoformat())
    return nxt.date()


def last_completed_trading_day() -> date:
    """
    The most recently completed trading day.
    If today is a trading day and it's after market close (used as a safe
    default), return today. Otherwise return the previous trading day.
    This is intentionally simple — flows can override trade_date explicitly.
    """
    today = date.today()
    if is_trading_day(today):
        return today
    return previous_trading_day(today)
