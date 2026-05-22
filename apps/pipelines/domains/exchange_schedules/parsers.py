"""Parsers for exchange schedule and holiday bronze records."""

import hashlib
import json
from datetime import date
from typing import Any

from providers.eodhd.models import ExchangeSchedule


def _bronze_row(payload: dict[str, Any], provider: str) -> dict[str, Any]:
    raw_json = json.dumps(payload, sort_keys=True)
    row_hash = hashlib.sha256(raw_json.encode()).hexdigest()
    return {**payload, "provider": provider, "raw_json": raw_json, "row_hash": row_hash}


def parse_schedule_record(schedule: ExchangeSchedule, snapshot_date: date) -> dict[str, Any]:
    """Flatten an ExchangeSchedule into a single bronze.exchange_schedules row."""
    h = schedule.trading_hours
    payload: dict[str, Any] = {
        "exchange_code": schedule.exchange_code,
        "name": schedule.name,
        "timezone": schedule.timezone,
        "session_open": h.session_open,
        "session_close": h.session_close,
        "working_days": h.working_days,
        "pre_market_open": h.pre_market_open,
        "pre_market_close": h.pre_market_close,
        "after_hours_open": h.after_hours_open,
        "after_hours_close": h.after_hours_close,
        "lunch_break_start": h.lunch_break_start,
        "lunch_break_end": h.lunch_break_end,
        "snapshot_date": snapshot_date.isoformat(),
    }
    return _bronze_row(payload, schedule.provider)


def parse_holiday_records(schedule: ExchangeSchedule, snapshot_date: date) -> list[dict[str, Any]]:
    """Expand ExchangeSchedule holidays into one bronze.exchange_holidays row per date."""
    records = []
    for holiday_date, holiday in schedule.exchange_holidays.items():
        payload: dict[str, Any] = {
            "exchange_code": schedule.exchange_code,
            "holiday_date": holiday_date,
            "holiday_name": holiday.holiday_name,
            "holiday_type": holiday.holiday_type,
            "early_close_time": holiday.early_close_time,
            "snapshot_date": snapshot_date.isoformat(),
        }
        records.append(_bronze_row(payload, schedule.provider))
    return records
