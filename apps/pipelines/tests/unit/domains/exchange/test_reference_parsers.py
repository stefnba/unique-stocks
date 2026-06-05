"""Tests for exchange reference parser output."""

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
from providers.eodhd.models import SupportedExchange
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
