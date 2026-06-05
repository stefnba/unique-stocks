"""Tests for instrument parser output."""

import json
from datetime import date

from domains.instrument.datasets import INSTRUMENT_DATASET
from domains.instrument.parsers import parse_instrument_snapshots
from providers.eodhd.models import Instrument


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
