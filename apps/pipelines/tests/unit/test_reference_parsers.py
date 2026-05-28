"""Tests for migrated reference-domain parser output."""

import json
from datetime import date

import pytest
from pydantic import ValidationError

from domains.exchange.datasets import EXCHANGE_CATALOG_DATASET, EXCHANGE_MIC_REGISTRY_DATASET
from domains.exchange.parsers import (
    load_iso10383_mic_csv,
    parse_exchange_catalog_snapshots,
    parse_exchange_mic_registry_snapshots,
    parse_iso10383_mic_raw_rows,
)
from domains.exchange_schedule.datasets import EXCHANGE_HOLIDAY_DATASET, EXCHANGE_SCHEDULE_DATASET
from domains.exchange_schedule.parsers import parse_exchange_holiday_snapshots, parse_exchange_schedule_snapshot
from domains.instrument.datasets import INSTRUMENT_DATASET
from domains.instrument.parsers import parse_instrument_snapshots
from providers.eodhd.models import ExchangeSchedule, Instrument, SupportedExchange
from providers.iso10383.models import ISO10383MICRaw


def test_parse_exchange_catalog_snapshots() -> None:
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
    sources = parse_exchange_catalog_snapshots([raw], date(2026, 5, 24))
    assert sources[0].row.provider_exchange_code == "US"
    assert sources[0].row.operating_mic_codes == "XNYS"
    assert sources[0].row.snapshot_date == date(2026, 5, 24)
    assert sources[0].raw_fragment is raw
    assert json.loads(EXCHANGE_CATALOG_DATASET.bronze_record(sources[0])["raw_json"])["Code"] == "US"


def test_parse_iso10383_mic_registry_rows() -> None:
    """ISO MIC registry rows parse lifecycle dates and hierarchy fields."""
    raw_rows = load_iso10383_mic_csv(
        '"MIC","OPERATING MIC","OPRT/SGMT","MARKET NAME-INSTITUTION DESCRIPTION",'
        '"LEGAL ENTITY NAME","LEI","MARKET CATEGORY CODE","ACRONYM",'
        '"ISO COUNTRY CODE (ISO 3166)","CITY","WEBSITE","STATUS","CREATION DATE",'
        '"LAST UPDATE DATE","LAST VALIDATION DATE","EXPIRY DATE","COMMENTS"\n'
        '"XCNQ","XCNQ","OPRT","CANADIAN SECURITIES EXCHANGE","CNSX MARKETS, INC.",'
        '"","RMKT","CSE LISTED","CA","TORONTO","WWW.THECSE.COM","ACTIVE",'
        '"20090427","20210927","20210927","",""\n'
        '"PURE","XCNQ","SGMT","CANADIAN SECURITIES EXCHANGE - PURE","CNSX MARKETS, INC.",'
        '"","NSPD","CSE-PURE","CA","TORONTO","WWW.THECSE.COM","ACTIVE",'
        '"20061225","20210927","20210927","","SEGMENT COMMENT."\n'
    )
    valid, rejected = parse_iso10383_mic_raw_rows(raw_rows)

    assert not rejected
    assert valid[0].creation_date == date(2009, 4, 27)
    assert valid[0].lei is None
    assert valid[0].expiry_date is None
    assert valid[1].mic == "PURE"
    assert valid[1].operating_mic == "XCNQ"
    assert valid[1].mic_type == "SGMT"

    sources = parse_exchange_mic_registry_snapshots(valid, date(2026, 5, 24))
    assert sources[0].row.provider_supported is True
    assert sources[1].row.operating_mic == "XCNQ"
    record = EXCHANGE_MIC_REGISTRY_DATASET.bronze_record(sources[0])
    assert record["data_provider"] == "iso10383"
    assert json.loads(record["raw_json"])["MIC"] == "XCNQ"


def test_iso10383_mic_model_forbids_unknown_fields() -> None:
    """ISO provider model catches unexpected CSV columns."""
    row = {
        "MIC": "XABC",
        "OPERATING MIC": "XABC",
        "OPRT/SGMT": "OPRT",
        "MARKET NAME-INSTITUTION DESCRIPTION": "Example Market",
        "LEGAL ENTITY NAME": "",
        "LEI": "",
        "MARKET CATEGORY CODE": "RMKT",
        "ACRONYM": "",
        "ISO COUNTRY CODE (ISO 3166)": "US",
        "CITY": "NEW YORK",
        "WEBSITE": "",
        "STATUS": "ACTIVE",
        "CREATION DATE": "20260101",
        "LAST UPDATE DATE": "",
        "LAST VALIDATION DATE": "",
        "EXPIRY DATE": "",
        "COMMENTS": "",
        "UNEXPECTED": "drift",
    }

    with pytest.raises(ValidationError):
        ISO10383MICRaw.model_validate(row)


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
    assert valid[0].row.provider_exchange_code == "US"
    assert valid[0].row.provider_listing_exchange_code == "NASDAQ"
    assert valid[0].row.ticker == "AAPL"
    assert valid[0].raw_fragment is raw
    assert json.loads(INSTRUMENT_DATASET.bronze_record(valid[0])["raw_json"])["Code"] == "AAPL"


def test_parse_instrument_snapshot_allows_missing_listing_exchange() -> None:
    """Provider instrument rows can omit the per-row listing exchange code."""
    raw = Instrument.model_validate(
        {
            "Code": "EUFUND123",
            "Name": "Example Fund",
            "Country": "LUX",
            "Exchange": None,
            "Currency": "EUR",
            "Type": "Fund",
            "Isin": None,
        }
    )
    valid, rejected = parse_instrument_snapshots([raw], "EUFUND", date(2026, 5, 24))
    assert not rejected
    assert valid[0].row.provider_exchange_code == "EUFUND"
    assert valid[0].row.provider_listing_exchange_code is None
    assert json.loads(INSTRUMENT_DATASET.bronze_record(valid[0])["raw_json"])["Exchange"] is None


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
