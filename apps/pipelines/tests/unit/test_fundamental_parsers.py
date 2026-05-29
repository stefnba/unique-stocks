"""Tests for fundamentals provider models and parsers."""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from domains.fundamental.parsers import (
    instrument_family_from_type,
    parse_fundamental_document,
    parse_stock_identity_snapshot,
    parse_stock_statement_facts,
)
from providers.eodhd.client import EODHDClient
from providers.eodhd.models import FundamentalRaw

SNAPSHOT_DATE = date(2026, 5, 29)
REAL_FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "eodhd" / "fundamental"


def _real_fixture_payload(name: str) -> dict[str, Any]:
    """Load a full provider payload fixture."""
    return json.loads((REAL_FIXTURE_ROOT / name).read_text())


def _qualified_ticker(raw: FundamentalRaw) -> str:
    """Return a usable qualified ticker for fixtures with no PrimaryTicker."""
    primary = raw.general.get("PrimaryTicker")
    if isinstance(primary, str) and primary:
        return primary
    return f"{raw.general['Code']}.{raw.general['Exchange']}"


def _stock_payload() -> dict[str, Any]:
    """Representative stock fundamentals payload with nested statement maps."""
    return {
        "General": {
            "Code": "AAPL",
            "Type": "Common Stock",
            "Name": "Apple Inc",
            "Exchange": "NASDAQ",
            "CurrencyCode": "USD",
            "CurrencyName": "US Dollar",
            "CountryName": "USA",
            "CountryISO": "US",
            "OpenFigi": "BBG000B9XRY4",
            "ISIN": "US0378331005",
            "LEI": "HWUPKR0MPOU8FGXBT394",
            "PrimaryTicker": "AAPL.US",
            "CUSIP": "037833100",
            "CIK": "0000320193",
            "EmployerIdNumber": "94-2404110",
            "FiscalYearEnd": "September",
            "IPODate": "1980-12-12",
            "Sector": "Technology",
            "Industry": "Consumer Electronics",
            "GicSector": "Information Technology",
            "GicGroup": "Technology Hardware & Equipment",
            "GicIndustry": "Technology Hardware, Storage & Peripherals",
            "GicSubIndustry": "Technology Hardware, Storage & Peripherals",
            "HomeCategory": "Domestic",
            "IsDelisted": False,
            "FullTimeEmployees": 166000,
            "WebURL": "https://www.apple.com",
            "LogoURL": "/img/logos/US/aapl.png",
            "UpdatedAt": "2026-05-28",
        },
        "Highlights": {"MarketCapitalization": 4565564915712},
        "Financials": {
            "Balance_Sheet": {
                "currency_symbol": "USD",
                "yearly": {
                    "2025-09-30": {
                        "date": "2025-09-30",
                        "filing_date": "2025-10-31",
                        "currency_symbol": "USD",
                        "totalAssets": "359241000000.00",
                        "intangibleAssets": None,
                        "badMetric": "not-a-number",
                    }
                },
                "quarterly": {
                    "2026-03-31": {
                        "date": "2026-03-31",
                        "filing_date": "2026-05-01",
                        "currency_symbol": "USD",
                        "totalAssets": "331233000000.00",
                    }
                },
            },
            "Cash_Flow": {
                "currency_symbol": "USD",
                "yearly": {
                    "2025-09-30": {
                        "date": "2025-09-30",
                        "filing_date": "2025-10-31",
                        "currency_symbol": "USD",
                        "totalCashFromOperatingActivities": "111482000000.00",
                    }
                },
            },
            "Income_Statement": {"currency_symbol": "USD", "yearly": {}, "quarterly": {}},
        },
    }


def test_fundamental_raw_forbids_unknown_top_level_sections() -> None:
    """Provider fundamentals model catches unexpected top-level drift."""
    payload = {**_stock_payload(), "UnexpectedSection": {}}

    with pytest.raises(ValidationError, match="UnexpectedSection"):
        FundamentalRaw.model_validate(payload)


def test_parse_document_and_stock_identity() -> None:
    """Stock fundamentals produce document metadata and a stock identity row."""
    raw = FundamentalRaw.model_validate(_stock_payload())

    document = parse_fundamental_document(raw, ticker="AAPL.US", snapshot_date=SNAPSHOT_DATE)
    identity = parse_stock_identity_snapshot(raw, ticker="AAPL.US", snapshot_date=SNAPSHOT_DATE)

    assert document.row.provider_exchange_code == "US"
    assert document.row.instrument_family == "stock"
    assert document.row.provider_updated_at == date(2026, 5, 28)
    assert document.row.payload_hash
    assert document.row.top_level_sections == ["Financials", "General", "Highlights"]
    assert identity is not None
    assert identity.row.cik == "0000320193"
    assert identity.row.ipo_date == date(1980, 12, 12)
    assert identity.row.is_delisted is False
    assert identity.row.full_time_employees == 166000


def test_parse_stock_statement_facts_long_form_with_rejections() -> None:
    """Financial statements flatten into long-form facts and isolate bad metrics."""
    raw = FundamentalRaw.model_validate(_stock_payload())

    facts, rejected = parse_stock_statement_facts(raw, ticker="AAPL.US", snapshot_date=SNAPSHOT_DATE)

    assert len(facts) == 3
    assert len(rejected) == 1
    assert rejected[0]["metric_name"] == "badMetric"
    total_assets = next(
        fact.row
        for fact in facts
        if fact.row.statement_type == "balance_sheet"
        and fact.row.period_type == "annual"
        and fact.row.metric_name == "totalAssets"
    )
    assert total_assets.period_end_date == date(2025, 9, 30)
    assert total_assets.filing_date == date(2025, 10, 31)
    assert total_assets.currency_symbol == "USD"
    assert total_assets.metric_value == Decimal("359241000000.00")


def test_aapl_fixture_slice_parses_balance_sheet_and_preserves_earnings_trend() -> None:
    """Real AAPL-shaped fixture covers statement and earnings trend nesting."""
    raw = FundamentalRaw.model_validate(_real_fixture_payload("common_stock_AAPL.json"))

    document = parse_fundamental_document(raw, ticker="AAPL.US", snapshot_date=SNAPSHOT_DATE)
    facts, rejected = parse_stock_statement_facts(raw, ticker="AAPL.US", snapshot_date=SNAPSHOT_DATE)

    assert "Earnings" in document.row.top_level_sections
    assert "Financials" in document.row.top_level_sections
    assert len(facts) > 1000
    assert rejected == []
    total_assets = next(
        fact.row
        for fact in facts
        if fact.row.metric_name == "totalAssets" and fact.row.period_end_date == date(2026, 3, 31)
    )
    assert total_assets.period_type == "quarterly"
    assert total_assets.metric_value == Decimal("371082000000.00")

    assert raw.earnings is not None
    trend = raw.earnings["Trend"]["Quarterly"]["2026-09-30"]
    assert trend["period"] == "+1q"
    assert trend["fiscalQuarter"] == "Q4"
    assert trend["revenueEstimateYearAgoEps"] is None


@pytest.mark.parametrize(
    ("fixture_name", "expected_family", "emits_stock_rows"),
    [
        ("common_stock_AAPL.json", "stock", True),
        ("common_stock_SIE.json", "stock", True),
        ("etf_DAXEX.json", "etf", False),
        ("fund_URNQX.json", "fund", False),
    ],
)
def test_real_fundamental_fixtures_validate_and_route(
    fixture_name: str,
    expected_family: str,
    emits_stock_rows: bool,
) -> None:
    """Full real fixtures validate and route through the current family parsers."""
    raw = FundamentalRaw.model_validate(_real_fixture_payload(fixture_name))
    ticker = _qualified_ticker(raw)

    document = parse_fundamental_document(raw, ticker=ticker, snapshot_date=SNAPSHOT_DATE)
    identity = parse_stock_identity_snapshot(raw, ticker=ticker, snapshot_date=SNAPSHOT_DATE)
    facts, rejected = parse_stock_statement_facts(raw, ticker=ticker, snapshot_date=SNAPSHOT_DATE)

    assert document.row.ticker == ticker
    assert document.row.code == raw.general["Code"]
    assert document.row.instrument_family == expected_family
    assert document.row.payload_hash
    assert document.row.top_level_sections
    assert rejected == []

    if emits_stock_rows:
        assert identity is not None
        assert len(facts) > 0
    else:
        assert identity is None
        assert facts == []


def test_non_stock_document_does_not_emit_stock_rows() -> None:
    """ETF/fund/index documents are routed without pretending they are stocks."""
    payload = {
        "General": {
            "Code": "SPY",
            "Type": "ETF",
            "Name": "SPDR S&P 500 ETF Trust",
            "Exchange": "NYSE ARCA",
            "PrimaryTicker": "SPY.US",
        },
        "ETF_Data": {"ISIN": "US78462F1030"},
    }
    raw = FundamentalRaw.model_validate(payload)

    document = parse_fundamental_document(raw, ticker="SPY.US", snapshot_date=SNAPSHOT_DATE)
    identity = parse_stock_identity_snapshot(raw, ticker="SPY.US", snapshot_date=SNAPSHOT_DATE)
    facts, rejected = parse_stock_statement_facts(raw, ticker="SPY.US", snapshot_date=SNAPSHOT_DATE)

    assert document.row.instrument_family == "etf"
    assert identity is None
    assert facts == []
    assert rejected == []


@pytest.mark.asyncio
async def test_eodhd_client_get_fundamental_uses_v11_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Client fundamentals calls use the recommended v1.1 endpoint."""
    calls: dict[str, Any] = {}
    expected = FundamentalRaw.model_validate(_stock_payload())

    async def fake_get(
        self: EODHDClient,
        path: str,
        *,
        model: type[FundamentalRaw],
        params: dict[str, Any] | None = None,
    ) -> FundamentalRaw:
        calls["path"] = path
        calls["model"] = model
        calls["params"] = params
        return expected

    monkeypatch.setattr(EODHDClient, "_get", fake_get)

    client = EODHDClient(api_key="demo")
    result = await client.get_fundamental("AAPL.US", filters=["General", "Financials::Balance_Sheet::yearly"])

    assert result is expected
    assert calls == {
        "path": "/v1.1/fundamentals/AAPL.US",
        "model": FundamentalRaw,
        "params": {"filter": "General,Financials::Balance_Sheet::yearly"},
    }


@pytest.mark.parametrize(
    ("provider_type", "family"),
    [
        ("Common Stock", "stock"),
        ("ETF", "etf"),
        ("Mutual Fund", "fund"),
        ("INDEX", "index"),
        ("Corporate Bond", "bond"),
        ("", "unknown"),
    ],
)
def test_instrument_family_from_type(provider_type: str, family: str) -> None:
    """Provider type text maps to stable instrument families."""
    assert instrument_family_from_type(provider_type) == family
