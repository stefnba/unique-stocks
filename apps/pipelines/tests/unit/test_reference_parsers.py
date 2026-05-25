"""Tests for migrated reference-domain parser output."""

import json
from datetime import date

from domains.exchange.datasets import EXCHANGE_DATASET
from domains.exchange.parsers import parse_exchange_snapshots
from domains.exchange_schedule.datasets import EXCHANGE_HOLIDAY_DATASET, EXCHANGE_SCHEDULE_DATASET
from domains.exchange_schedule.parsers import parse_exchange_holiday_snapshots, parse_exchange_schedule_snapshot
from domains.instrument.datasets import INSTRUMENT_DATASET
from domains.instrument.parsers import parse_instrument_snapshots
from providers.eodhd.models import ExchangeSchedule, Instrument, SupportedExchange


def test_parse_exchange_snapshots() -> None:
    """Supported exchange parse to typed Bronze sources."""
    raw = SupportedExchange.model_validate(
        {
            "Code": "US",
            "Name": "USA Stocks",
            "OperatingMIC": "XNYS",
            "Country": "USA",
            "Currency": "USD",
            "CountryISO2": "US",
            "CountryISO3": "USA",
        }
    )
    sources = parse_exchange_snapshots([raw], date(2026, 5, 24))
    assert sources[0].row.exchange_code == "US"
    assert sources[0].row.snapshot_date == date(2026, 5, 24)
    assert sources[0].raw_fragment is raw
    assert json.loads(EXCHANGE_DATASET.bronze_record(sources[0])["raw_json"])["Code"] == "US"


def test_parse_instrument_snapshots() -> None:
    """Instrument parse to typed Bronze results."""
    raw = Instrument.model_validate(
        {
            "Code": "AAPL",
            "Name": "Apple Inc",
            "Country": "USA",
            "Exchange": "NASDAQ",
            "Currency": "USD",
            "Type": "Common Stock",
            "Isin": "US0378331005",
        }
    )
    valid, rejected = parse_instrument_snapshots([raw], "US", date(2026, 5, 24))
    assert not rejected
    assert valid[0].row.exchange_code == "US"
    assert valid[0].row.ticker == "AAPL"
    assert valid[0].raw_fragment is raw
    assert json.loads(INSTRUMENT_DATASET.bronze_record(valid[0])["raw_json"])["Code"] == "AAPL"


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
    assert source.row.exchange_code == "XNYS"
    assert source.row.session_open == "09:30"
    assert source.raw_fragment.exchange_code == "XNYS"
    assert json.loads(EXCHANGE_SCHEDULE_DATASET.bronze_record(source)["raw_json"])["Code"] == "XNYS"


def test_parse_exchange_holiday_snapshots() -> None:
    """Exchange holiday use a small provider fragment plus parent context."""
    sources = parse_exchange_holiday_snapshots(_schedule(), date(2026, 5, 24))
    assert sources[0].row.holiday_date == date(2026, 1, 1)
    assert sources[0].raw_fragment["exchange_code"] == "XNYS"
    assert sources[0].raw_fragment["Holiday"] == "New Year's Day"
    assert json.loads(EXCHANGE_HOLIDAY_DATASET.bronze_record(sources[0])["raw_json"])["Holiday"] == "New Year's Day"
