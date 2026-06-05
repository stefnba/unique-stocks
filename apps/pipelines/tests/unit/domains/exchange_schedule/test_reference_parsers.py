"""Tests for exchange schedule parser output."""

import json
from datetime import date

from domains.exchange_schedule.datasets import EXCHANGE_HOLIDAY_DATASET, EXCHANGE_SCHEDULE_DATASET
from domains.exchange_schedule.parsers import parse_exchange_holiday_snapshots, parse_exchange_schedule_snapshot
from providers.eodhd.models import ExchangeSchedule


def _schedule() -> ExchangeSchedule:
    return ExchangeSchedule.model_validate(
        {
            "Name": "NYSE",
            "Code": "XNYS",
            "Timezone": "America/New_York",
            "TradingHours": {
                "Open": "09:30",
                "Close": "16:00",
                "WorkingDays": "Mon,Tue,Wed,Thu,Fri",
            },
            "ExchangeHolidays": {
                "2026-01-01": {
                    "Holiday": "New Year's Day",
                    "Type": "official",
                    "EarlyClose": None,
                }
            },
        }
    )


def test_parse_exchange_schedule_snapshot() -> None:
    """Exchange schedule parse to a single typed Bronze result."""
    source = parse_exchange_schedule_snapshot(_schedule(), date(2026, 5, 24))
    assert source.row.provider_schedule_exchange_code == "XNYS"
    assert source.row.session_open == "09:30"
    assert source.raw_fragment.provider_schedule_exchange_code == "XNYS"
    assert json.loads(EXCHANGE_SCHEDULE_DATASET.bronze_record(source)["raw_json"])["Code"] == "XNYS"


def test_parse_exchange_holiday_snapshots() -> None:
    """Exchange holiday use a small provider fragment plus parent context."""
    sources = parse_exchange_holiday_snapshots(_schedule(), date(2026, 5, 24))
    assert sources[0].row.holiday_date == date(2026, 1, 1)
    assert sources[0].raw_fragment["provider_schedule_exchange_code"] == "XNYS"
    assert sources[0].raw_fragment["Holiday"] == "New Year's Day"
    assert json.loads(EXCHANGE_HOLIDAY_DATASET.bronze_record(sources[0])["raw_json"])["Holiday"] == "New Year's Day"
