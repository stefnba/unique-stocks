"""Parsers for exchange schedule and holiday Bronze rows."""

from datetime import date
from typing import Any

from core.ingestion import BronzeParseResult
from domains.exchange_schedule.models import ExchangeHolidaySnapshot, ExchangeScheduleSnapshot
from providers.eodhd.models import ExchangeSchedule


def parse_exchange_schedule_snapshot(
    schedule: ExchangeSchedule,
    snapshot_date: date,
) -> BronzeParseResult[ExchangeScheduleSnapshot]:
    """Flatten an ExchangeSchedule into one Bronze exchange schedule row."""
    h = schedule.trading_hours
    row = ExchangeScheduleSnapshot(
        exchange_code=schedule.exchange_code,
        name=schedule.name,
        timezone=schedule.timezone,
        session_open=h.session_open,
        session_close=h.session_close,
        working_days=h.working_days,
        pre_market_open=h.pre_market_open,
        pre_market_close=h.pre_market_close,
        after_hours_open=h.after_hours_open,
        after_hours_close=h.after_hours_close,
        lunch_break_start=h.lunch_break_start,
        lunch_break_end=h.lunch_break_end,
        snapshot_date=snapshot_date,
    )
    return BronzeParseResult(row=row, raw_fragment=schedule)


def parse_exchange_holiday_snapshots(
    schedule: ExchangeSchedule,
    snapshot_date: date,
) -> list[BronzeParseResult[ExchangeHolidaySnapshot]]:
    """Expand ExchangeSchedule holiday into one Bronze exchange holiday row per date."""
    records = []
    for holiday_date, holiday in schedule.exchange_holiday.items():
        row = ExchangeHolidaySnapshot(
            exchange_code=schedule.exchange_code,
            holiday_date=date.fromisoformat(holiday_date),
            holiday_name=holiday.holiday_name,
            holiday_type=holiday.holiday_type,
            early_close_time=holiday.early_close_time,
            snapshot_date=snapshot_date,
        )
        raw_fragment: dict[str, Any] = {
            "exchange_code": schedule.exchange_code,
            "snapshot_date": snapshot_date,
            "holiday_date": holiday_date,
            **holiday.model_dump(mode="json", by_alias=True),
        }
        records.append(BronzeParseResult(row=row, raw_fragment=raw_fragment))
    return records
