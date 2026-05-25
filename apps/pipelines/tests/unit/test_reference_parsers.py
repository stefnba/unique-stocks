"""Tests for migrated reference-domain parser output."""

import json
from datetime import date

from domains.exchange.datasets import EXCHANGE_DATASET
from domains.exchange.parsers import parse_exchange_snapshots
from providers.eodhd.models import SupportedExchange


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
