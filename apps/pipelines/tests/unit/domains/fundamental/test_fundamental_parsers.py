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
    parse_etf_holdings,
    parse_etf_identity_snapshot,
    parse_fund_metric_facts,
    parse_fundamental_document,
    parse_index_components,
    parse_index_historical_components,
    parse_index_identity_snapshot,
    parse_mutual_fund_holdings,
    parse_mutual_fund_identity_snapshot,
    parse_stock_dividend_counts,
    parse_stock_earnings_facts,
    parse_stock_esg_activities,
    parse_stock_holders,
    parse_stock_identity_snapshot,
    parse_stock_insider_transactions,
    parse_stock_metric_facts,
    parse_stock_outstanding_shares,
    parse_stock_shares_stats_snapshot,
    parse_stock_splits_dividends_snapshot,
    parse_stock_statement_facts,
)
from providers.eodhd.client import EODHDClient
from providers.eodhd.models import FundamentalRaw

SNAPSHOT_DATE = date(2026, 5, 29)
TESTS_ROOT = Path(__file__).resolve().parents[3]
REAL_FIXTURE_ROOT = TESTS_ROOT / "fixtures" / "eodhd" / "fundamental"


def _real_fixture_payload(name: str) -> dict[str, Any]:
    """Load a full provider payload fixture."""
    return json.loads((REAL_FIXTURE_ROOT / name).read_text())


def _api_symbol_from_raw(raw: FundamentalRaw) -> str:
    """Return a usable API symbol for fixtures with no PrimaryTicker."""
    primary = raw.general.get("PrimaryTicker")
    if isinstance(primary, str) and primary:
        return primary
    return f"{raw.general['Code']}.{raw.general['Exchange']}"


def _provider_ref(api_symbol: str) -> dict[str, str]:
    """Split an EODHD API symbol into parser identity kwargs."""
    provider_instrument_code, provider_exchange_code = api_symbol.rsplit(".", maxsplit=1)
    return {
        "provider_exchange_code": provider_exchange_code,
        "provider_instrument_code": provider_instrument_code,
    }


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


def _stock_earnings_payload() -> dict[str, Any]:
    """Representative stock earnings payload with annual, history, and trend maps."""
    return {
        "General": {
            "Code": "AAPL",
            "Type": "Common Stock",
            "Name": "Apple Inc",
            "Exchange": "NASDAQ",
            "PrimaryTicker": "AAPL.US",
        },
        "Earnings": {
            "Annual": {
                "2025-09-30": {
                    "date": "2025-09-30",
                    "epsActual": "6.11",
                }
            },
            "History": {
                "2026-03-31": {
                    "reportDate": "2026-05-01",
                    "date": "2026-03-31",
                    "beforeAfterMarket": "AfterMarket",
                    "currency": "USD",
                    "epsActual": 1.65,
                    "epsEstimate": "1.62",
                    "epsDifference": "0.03",
                    "surprisePercent": "1.85",
                    "badMetric": "not-a-number",
                }
            },
            "Trend": {
                "Quarterly": {
                    "2026-09-30": {
                        "date": "2026-09-30",
                        "period": "+1q",
                        "earningsEstimateAvg": "2.0107",
                        "revenueEstimateAvg": "114207466420.00",
                        "epsRevisionsDownLast7days": None,
                        "type": "quarterly",
                        "fiscalQuarter": "Q4",
                    }
                },
                "Annual": {
                    "2027-09-30": {
                        "date": "2027-09-30",
                        "period": "+1y",
                        "earningsEstimateAvg": "9.6552",
                        "type": "yearly",
                    }
                },
            },
        },
    }


def _stock_shares_payload() -> dict[str, Any]:
    """Representative stock share-statistics and outstanding-shares payload."""
    return {
        "General": {
            "Code": "AAPL",
            "Type": "Common Stock",
            "Name": "Apple Inc",
            "Exchange": "NASDAQ",
            "PrimaryTicker": "AAPL.US",
        },
        "SharesStats": {
            "SharesOutstanding": 14687356000,
            "SharesFloat": "14662387495",
            "PercentInsiders": 1.633,
            "PercentInstitutions": "65.801",
            "SharesShort": None,
            "SharesShortPriorMonth": None,
            "ShortRatio": None,
            "ShortPercentOutstanding": None,
            "ShortPercentFloat": "0.0092",
        },
        "outstandingShares": {
            "annual": {
                "0": {
                    "date": "2026",
                    "dateFormatted": "2026-12-31",
                    "sharesMln": "14768.1150",
                    "shares": 14768115000,
                }
            },
            "quarterly": {
                "0": {
                    "date": "2026-Q1",
                    "dateFormatted": "2026-03-31",
                    "sharesMln": "14768.1150",
                    "shares": 14768115000,
                },
                "1": "bad-row",
            },
        },
    }


def _stock_remaining_payload() -> dict[str, Any]:
    """Representative remaining stock fundamentals sections."""
    return {
        "General": {
            "Code": "AAPL",
            "Type": "Common Stock",
            "Name": "Apple Inc",
            "Exchange": "NASDAQ",
            "PrimaryTicker": "AAPL.US",
        },
        "Holders": {
            "Institutions": {
                "0": {
                    "name": "Vanguard Group Inc",
                    "date": "2025-12-31",
                    "totalShares": 9.711,
                    "totalAssets": 5.6215,
                    "currentShares": 1426283914,
                    "change": 26856752,
                    "change_p": 1.9191,
                }
            },
            "Funds": {
                "0": {
                    "name": "Vanguard Total Stock Mkt Idx Inv",
                    "date": "2026-04-30",
                    "totalShares": "3.185",
                    "totalAssets": "5.7391",
                    "currentShares": 467796005,
                    "change": 1441658,
                    "change_p": 0.3091,
                },
                "1": "bad-row",
            },
        },
        "InsiderTransactions": {
            "0": {
                "date": "2026-03-30",
                "ownerCik": None,
                "ownerName": "Jane Director",
                "transactionDate": "2026-03-30",
                "transactionCode": "S",
                "transactionAmount": 25809,
                "transactionPrice": 359.33,
                "transactionAcquiredDisposed": "D",
                "postTransactionAmount": None,
                "secLink": "https://www.sec.gov/example.xml",
            },
            "1": "bad-row",
        },
        "SplitsDividends": {
            "ForwardAnnualDividendRate": 1.08,
            "ForwardAnnualDividendYield": 0.0035,
            "PayoutRatio": 0.127,
            "DividendDate": "2026-05-14",
            "ExDividendDate": "2026-05-11",
            "LastSplitFactor": "4:1",
            "LastSplitDate": "2020-08-31",
            "NumberDividendsByYear": {
                "0": {"Year": 2025, "Count": 4},
                "1": "bad-row",
            },
        },
        "Highlights": {
            "MarketCapitalization": 4565564915712,
            "MostRecentQuarter": "2026-03-31",
        },
        "Valuation": {"TrailingPE": 37.6332},
        "Technicals": {"Beta": 1.065, "52WeekHigh": 313.26},
        "AnalystRatings": {"Rating": 4.0417, "StrongBuy": 23},
        "ESGScores": {
            "RatingDate": "2019-01-01",
            "TotalEsg": 26.15,
            "Disclaimer": "Beta",
            "ActivitiesInvolvement": {
                "0": {"Activity": "adult", "Involvement": "No"},
                "1": {"Activity": "animalTesting", "Involvement": "No"},
            },
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

    document = parse_fundamental_document(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    identity = parse_stock_identity_snapshot(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)

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

    facts, rejected = parse_stock_statement_facts(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)

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


def test_parse_stock_earnings_facts_long_form_with_rejections() -> None:
    """Earnings annual, history, and trend maps flatten into long-form facts."""
    raw = FundamentalRaw.model_validate(_stock_earnings_payload())

    facts, rejected = parse_stock_earnings_facts(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)

    assert len(facts) == 8
    assert len(rejected) == 1
    assert rejected[0]["earnings_section"] == "history"
    assert rejected[0]["metric_name"] == "badMetric"

    history_actual = next(
        fact.row for fact in facts if fact.row.earnings_section == "history" and fact.row.metric_name == "epsActual"
    )
    assert history_actual.period_type is None
    assert history_actual.fiscal_period_end == date(2026, 3, 31)
    assert history_actual.report_date == date(2026, 5, 1)
    assert history_actual.before_after_market == "AfterMarket"
    assert history_actual.currency_code == "USD"
    assert history_actual.metric_value == Decimal("1.65")

    trend_avg = next(
        fact.row
        for fact in facts
        if fact.row.earnings_section == "trend"
        and fact.row.period_type == "quarterly"
        and fact.row.metric_name == "earningsEstimateAvg"
    )
    assert trend_avg.fiscal_quarter == "Q4"
    assert trend_avg.period_offset == "+1q"
    assert trend_avg.metric_value == Decimal("2.0107")


def test_parse_stock_shares_stats_and_outstanding_shares() -> None:
    """SharesStats and outstandingShares produce separate Bronze rows."""
    raw = FundamentalRaw.model_validate(_stock_shares_payload())

    shares_stats = parse_stock_shares_stats_snapshot(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    outstanding_shares, rejected = parse_stock_outstanding_shares(
        raw,
        **_provider_ref("AAPL.US"),
        snapshot_date=SNAPSHOT_DATE,
    )

    assert shares_stats is not None
    assert shares_stats.row.shares_outstanding == Decimal("14687356000")
    assert shares_stats.row.shares_float == Decimal("14662387495")
    assert shares_stats.row.percent_insiders == Decimal("1.633")
    assert shares_stats.row.short_percent_float == Decimal("0.0092")
    assert shares_stats.row.shares_short is None

    assert len(outstanding_shares) == 2
    assert len(rejected) == 1
    assert rejected[0]["section"] == "outstanding_shares"
    assert rejected[0]["period_type"] == "quarterly"
    annual = next(row.row for row in outstanding_shares if row.row.period_type == "annual")
    assert annual.provider_period_label == "2026"
    assert annual.period_end_date == date(2026, 12, 31)
    assert annual.shares_mln == Decimal("14768.1150")
    assert annual.shares == Decimal("14768115000")
    quarterly = next(row.row for row in outstanding_shares if row.row.period_type == "quarterly")
    assert quarterly.provider_period_label == "2026-Q1"
    assert quarterly.period_end_date == date(2026, 3, 31)


def test_parse_remaining_stock_sections() -> None:
    """Holders, insider transactions, splits/dividends, compact metrics, and ESG activities parse cleanly."""
    raw = FundamentalRaw.model_validate(_stock_remaining_payload())

    holders, holder_rejected = parse_stock_holders(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    insider_transactions, insider_rejected = parse_stock_insider_transactions(
        raw,
        **_provider_ref("AAPL.US"),
        snapshot_date=SNAPSHOT_DATE,
    )
    splits = parse_stock_splits_dividends_snapshot(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    dividend_counts, dividend_rejected = parse_stock_dividend_counts(
        raw,
        **_provider_ref("AAPL.US"),
        snapshot_date=SNAPSHOT_DATE,
    )
    metric_facts = parse_stock_metric_facts(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    esg_activities, esg_rejected = parse_stock_esg_activities(
        raw,
        **_provider_ref("AAPL.US"),
        snapshot_date=SNAPSHOT_DATE,
    )

    assert len(holders) == 2
    assert len(holder_rejected) == 1
    institution = next(holder.row for holder in holders if holder.row.holder_type == "institution")
    assert institution.holder_name == "Vanguard Group Inc"
    assert institution.report_date == date(2025, 12, 31)
    assert institution.current_shares == Decimal("1426283914")
    assert institution.shares_change_percent == Decimal("1.9191")
    assert len(insider_transactions) == 1
    assert len(insider_rejected) == 1
    insider = insider_transactions[0].row
    assert insider.provider_position == 0
    assert insider.filing_date == date(2026, 3, 30)
    assert insider.owner_name == "Jane Director"
    assert insider.transaction_date == date(2026, 3, 30)
    assert insider.transaction_code == "S"
    assert insider.transaction_amount == Decimal("25809")
    assert insider.transaction_price == Decimal("359.33")
    assert insider.transaction_acquired_disposed == "D"
    assert insider.sec_link == "https://www.sec.gov/example.xml"

    assert splits is not None
    assert splits.row.forward_annual_dividend_rate == Decimal("1.08")
    assert splits.row.dividend_date == date(2026, 5, 14)
    assert splits.row.last_split_factor == "4:1"
    assert splits.row.last_split_date == date(2020, 8, 31)
    assert len(dividend_counts) == 1
    assert len(dividend_rejected) == 1
    assert dividend_counts[0].row.year == 2025
    assert dividend_counts[0].row.dividend_count == 4

    market_cap = next(fact.row for fact in metric_facts if fact.row.metric_name == "MarketCapitalization")
    assert market_cap.metric_group == "highlights"
    assert market_cap.metric_value == Decimal("4565564915712")
    assert market_cap.metric_date == date(2026, 3, 31)
    assert next(fact.row for fact in metric_facts if fact.row.metric_name == "TrailingPE").metric_group == "valuation"
    assert next(fact.row for fact in metric_facts if fact.row.metric_name == "TotalEsg").metric_group == "esg_scores"

    assert len(esg_activities) == 2
    assert esg_rejected == []
    assert esg_activities[1].row.activity == "animalTesting"
    assert esg_activities[1].row.involvement == "No"
    assert esg_activities[1].row.rating_date == date(2019, 1, 1)


def test_parse_stock_splits_dividends_treats_provider_zero_date_as_missing() -> None:
    """EODHD uses 0000-00-00 as a missing date sentinel for some split fields."""
    payload = _stock_remaining_payload()
    payload["SplitsDividends"]["LastSplitDate"] = "0000-00-00"
    raw = FundamentalRaw.model_validate(payload)

    splits = parse_stock_splits_dividends_snapshot(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)

    assert splits is not None
    assert splits.row.last_split_date is None


def test_aapl_fixture_slice_parses_balance_sheet_and_preserves_earnings_trend() -> None:
    """Real AAPL-shaped fixture covers statement and earnings trend nesting."""
    raw = FundamentalRaw.model_validate(_real_fixture_payload("common_stock_AAPL.json"))

    document = parse_fundamental_document(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    facts, rejected = parse_stock_statement_facts(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    earnings_facts, earnings_rejected = parse_stock_earnings_facts(
        raw,
        **_provider_ref("AAPL.US"),
        snapshot_date=SNAPSHOT_DATE,
    )
    shares_stats = parse_stock_shares_stats_snapshot(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    outstanding_shares, outstanding_rejected = parse_stock_outstanding_shares(
        raw,
        **_provider_ref("AAPL.US"),
        snapshot_date=SNAPSHOT_DATE,
    )
    holders, holder_rejected = parse_stock_holders(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    insider_transactions, insider_rejected = parse_stock_insider_transactions(
        raw,
        **_provider_ref("AAPL.US"),
        snapshot_date=SNAPSHOT_DATE,
    )
    splits = parse_stock_splits_dividends_snapshot(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    dividend_counts, dividend_rejected = parse_stock_dividend_counts(
        raw,
        **_provider_ref("AAPL.US"),
        snapshot_date=SNAPSHOT_DATE,
    )
    metric_facts = parse_stock_metric_facts(raw, **_provider_ref("AAPL.US"), snapshot_date=SNAPSHOT_DATE)
    esg_activities, esg_rejected = parse_stock_esg_activities(
        raw,
        **_provider_ref("AAPL.US"),
        snapshot_date=SNAPSHOT_DATE,
    )

    assert "Earnings" in document.row.top_level_sections
    assert "Financials" in document.row.top_level_sections
    assert len(facts) > 1000
    assert rejected == []
    assert len(earnings_facts) > 1000
    assert earnings_rejected == []
    assert shares_stats is not None
    assert shares_stats.row.shares_outstanding == Decimal("14687356000")
    assert len(outstanding_shares) == 205
    assert outstanding_rejected == []
    assert len(holders) == 40
    assert holder_rejected == []
    assert insider_transactions == []
    assert insider_rejected == []
    assert splits is not None
    assert splits.row.last_split_factor == "4:1"
    assert len(dividend_counts) == 24
    assert dividend_rejected == []
    assert len(metric_facts) > 0
    assert len(esg_activities) == 15
    assert esg_rejected == []
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
    earnings_estimate = next(
        fact.row
        for fact in earnings_facts
        if fact.row.earnings_section == "trend"
        and fact.row.period_type == "quarterly"
        and fact.row.fiscal_period_end == date(2026, 9, 30)
        and fact.row.metric_name == "earningsEstimateAvg"
    )
    assert earnings_estimate.metric_value == Decimal("2.0107")


@pytest.mark.parametrize(
    ("fixture_name", "expected_family", "emits_stock_rows"),
    [
        ("common_stock_AAPL.json", "stock", True),
        ("common_stock_SIE.json", "stock", True),
        ("common_stock_TSLA.json", "stock", True),
        ("etf_DAXEX.json", "etf", False),
        ("fund_URNQX.json", "fund", False),
        ("index_GDAXI.json", "index", False),
        ("index_GSPC.json", "index", False),
    ],
)
def test_real_fundamental_fixtures_validate_and_route(
    fixture_name: str,
    expected_family: str,
    emits_stock_rows: bool,
) -> None:
    """Full real fixtures validate and route through the current family parsers."""
    raw = FundamentalRaw.model_validate(_real_fixture_payload(fixture_name))
    api_symbol = _api_symbol_from_raw(raw)

    document = parse_fundamental_document(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    identity = parse_stock_identity_snapshot(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    facts, rejected = parse_stock_statement_facts(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    earnings_facts, earnings_rejected = parse_stock_earnings_facts(
        raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE
    )
    shares_stats = parse_stock_shares_stats_snapshot(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    outstanding_shares, outstanding_rejected = parse_stock_outstanding_shares(
        raw,
        **_provider_ref(api_symbol),
        snapshot_date=SNAPSHOT_DATE,
    )
    holders, holder_rejected = parse_stock_holders(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    insider_transactions, insider_rejected = parse_stock_insider_transactions(
        raw,
        **_provider_ref(api_symbol),
        snapshot_date=SNAPSHOT_DATE,
    )
    splits = parse_stock_splits_dividends_snapshot(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    dividend_counts, dividend_rejected = parse_stock_dividend_counts(
        raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE
    )
    metric_facts = parse_stock_metric_facts(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    esg_activities, esg_rejected = parse_stock_esg_activities(
        raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE
    )
    etf_identity = parse_etf_identity_snapshot(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    mutual_fund_identity = parse_mutual_fund_identity_snapshot(
        raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE
    )
    index_identity = parse_index_identity_snapshot(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)

    provider_ref = _provider_ref(api_symbol)
    assert document.row.provider_exchange_code == provider_ref["provider_exchange_code"]
    assert document.row.provider_instrument_code == provider_ref["provider_instrument_code"]
    assert document.row.instrument_family == expected_family
    assert document.row.payload_hash
    assert document.row.top_level_sections
    assert rejected == []
    assert earnings_rejected == []
    assert outstanding_rejected == []
    assert holder_rejected == []
    assert insider_rejected == []
    assert dividend_rejected == []
    assert esg_rejected == []

    if emits_stock_rows:
        assert identity is not None
        assert len(facts) > 0
        assert len(earnings_facts) > 0
        assert shares_stats is not None
        assert len(outstanding_shares) > 0
        assert splits is not None
        if fixture_name != "common_stock_TSLA.json":
            assert len(dividend_counts) > 0
        assert len(metric_facts) > 0
        assert len(esg_activities) > 0
        if fixture_name == "common_stock_TSLA.json":
            assert len(insider_transactions) == 20
    else:
        assert identity is None
        assert facts == []
        assert earnings_facts == []
        assert shares_stats is None
        assert outstanding_shares == []
        assert holders == []
        assert insider_transactions == []
        assert splits is None
        assert dividend_counts == []
        assert metric_facts == []
        assert esg_activities == []
        assert (etf_identity is not None) is (expected_family == "etf")
        assert (mutual_fund_identity is not None) is (expected_family == "fund")
        assert (index_identity is not None) is (expected_family == "index")


def test_etf_fixture_parses_identity_holdings_and_metrics() -> None:
    """ETF fixture emits ETF identity, holding edges, and fund metric facts."""
    raw = FundamentalRaw.model_validate(_real_fixture_payload("etf_DAXEX.json"))
    api_symbol = _api_symbol_from_raw(raw)

    identity = parse_etf_identity_snapshot(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    holdings, rejected = parse_etf_holdings(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    metric_facts = parse_fund_metric_facts(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)

    assert identity is not None
    assert identity.row.isin == "DE0005933931"
    assert identity.row.domicile == "Germany"
    assert identity.row.index_name == "Morningstar Germany TME NR EUR"
    assert identity.row.inception_date == date(2000, 12, 27)
    assert identity.row.holdings_count == 10
    assert len(holdings) == 10
    assert rejected == []
    first = holdings[0].row
    assert first.holding_provider_key == "SIE.XETRA"
    assert first.holding_provider_instrument_code == "SIE"
    assert first.holding_provider_exchange_code == "XETRA"
    assert first.assets_percent == Decimal("10.94759")
    assert first.is_top_10 is True
    assert len(metric_facts) > 0
    assert any(fact.row.metric_group == "Asset_Allocation" for fact in metric_facts)
    assert any(fact.row.metric_group == "Performance" for fact in metric_facts)


def test_mutual_fund_fixture_parses_identity_holdings_and_metrics() -> None:
    """Mutual fund fixture emits fund identity, top holdings, and metric facts."""
    raw = FundamentalRaw.model_validate(_real_fixture_payload("fund_URNQX.json"))
    api_symbol = _api_symbol_from_raw(raw)

    identity = parse_mutual_fund_identity_snapshot(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    holdings, rejected = parse_mutual_fund_holdings(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)
    metric_facts = parse_fund_metric_facts(raw, **_provider_ref(api_symbol), snapshot_date=SNAPSHOT_DATE)

    assert identity is not None
    assert identity.row.fund_category == "Large Growth"
    assert identity.row.fund_style == "Large Growth"
    assert len(holdings) == 10
    assert rejected == []
    assert holdings[0].row.holding_name == "NVIDIA CORP"
    assert holdings[0].row.weight_percent == Decimal("8.90")
    assert len(metric_facts) > 0
    assert any(fact.row.metric_group == "Asset_Allocation" for fact in metric_facts)
    assert any(fact.row.metric_group == "Value_Growth" for fact in metric_facts)


def test_index_fixtures_parse_identity_and_components() -> None:
    """Index fixtures emit index identity, current components, and historical components."""
    dax = FundamentalRaw.model_validate(_real_fixture_payload("index_GDAXI.json"))
    spx = FundamentalRaw.model_validate(_real_fixture_payload("index_GSPC.json"))

    dax_identity = parse_index_identity_snapshot(dax, **_provider_ref("GDAXI.INDX"), snapshot_date=SNAPSHOT_DATE)
    dax_components, dax_rejected = parse_index_components(
        dax, **_provider_ref("GDAXI.INDX"), snapshot_date=SNAPSHOT_DATE
    )
    spx_identity = parse_index_identity_snapshot(spx, **_provider_ref("GSPC.INDX"), snapshot_date=SNAPSHOT_DATE)
    spx_components, spx_rejected = parse_index_components(
        spx, **_provider_ref("GSPC.INDX"), snapshot_date=SNAPSHOT_DATE
    )
    historical, historical_rejected = parse_index_historical_components(
        spx,
        **_provider_ref("GSPC.INDX"),
        snapshot_date=SNAPSHOT_DATE,
    )

    assert dax_identity is not None
    assert dax_identity.row.currency_code == "EUR"
    assert len(dax_components) == 39
    assert dax_rejected == []
    assert dax_components[0].row.component_provider_instrument_code == "RWE"
    assert dax_components[0].row.component_provider_exchange_code == "XETRA"

    assert spx_identity is not None
    assert spx_identity.row.market_cap == Decimal("64030364996329")
    assert len(spx_components) == 503
    assert spx_rejected == []
    assert spx_components[0].row.component_provider_instrument_code == "AIZ"
    assert spx_components[0].row.weight == Decimal("0.0002")
    assert len(historical) == 812
    assert historical_rejected == []
    assert historical[0].row.component_provider_instrument_code == "A"
    assert historical[0].row.start_date == date(2000, 6, 5)
    assert historical[0].row.is_active_now is True


def test_tsla_fixture_parses_insider_transactions() -> None:
    """TSLA fixture emits stock insider transaction rows."""
    raw = FundamentalRaw.model_validate(_real_fixture_payload("common_stock_TSLA.json"))

    transactions, rejected = parse_stock_insider_transactions(
        raw, **_provider_ref("TSLA.US"), snapshot_date=SNAPSHOT_DATE
    )

    assert len(transactions) == 20
    assert rejected == []
    first = transactions[0].row
    assert first.provider_position == 0
    assert first.filing_date == date(2026, 3, 30)
    assert first.owner_name == "Kathleen Wilson-Thompson"
    assert first.transaction_date == date(2026, 3, 30)
    assert first.transaction_code == "S"
    assert first.transaction_amount == Decimal("25809")
    assert first.transaction_price == Decimal("359.33")
    assert first.transaction_acquired_disposed == "D"
    assert first.sec_link is not None


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

    document = parse_fundamental_document(raw, **_provider_ref("SPY.US"), snapshot_date=SNAPSHOT_DATE)
    identity = parse_stock_identity_snapshot(raw, **_provider_ref("SPY.US"), snapshot_date=SNAPSHOT_DATE)
    facts, rejected = parse_stock_statement_facts(raw, **_provider_ref("SPY.US"), snapshot_date=SNAPSHOT_DATE)
    earnings_facts, earnings_rejected = parse_stock_earnings_facts(
        raw,
        **_provider_ref("SPY.US"),
        snapshot_date=SNAPSHOT_DATE,
    )
    shares_stats = parse_stock_shares_stats_snapshot(raw, **_provider_ref("SPY.US"), snapshot_date=SNAPSHOT_DATE)
    outstanding_shares, outstanding_rejected = parse_stock_outstanding_shares(
        raw,
        **_provider_ref("SPY.US"),
        snapshot_date=SNAPSHOT_DATE,
    )
    holders, holder_rejected = parse_stock_holders(raw, **_provider_ref("SPY.US"), snapshot_date=SNAPSHOT_DATE)
    insider_transactions, insider_rejected = parse_stock_insider_transactions(
        raw,
        **_provider_ref("SPY.US"),
        snapshot_date=SNAPSHOT_DATE,
    )
    splits = parse_stock_splits_dividends_snapshot(raw, **_provider_ref("SPY.US"), snapshot_date=SNAPSHOT_DATE)
    dividend_counts, dividend_rejected = parse_stock_dividend_counts(
        raw,
        **_provider_ref("SPY.US"),
        snapshot_date=SNAPSHOT_DATE,
    )
    metric_facts = parse_stock_metric_facts(raw, **_provider_ref("SPY.US"), snapshot_date=SNAPSHOT_DATE)
    esg_activities, esg_rejected = parse_stock_esg_activities(
        raw,
        **_provider_ref("SPY.US"),
        snapshot_date=SNAPSHOT_DATE,
    )

    assert document.row.instrument_family == "etf"
    assert identity is None
    assert facts == []
    assert rejected == []
    assert earnings_facts == []
    assert earnings_rejected == []
    assert shares_stats is None
    assert outstanding_shares == []
    assert outstanding_rejected == []
    assert holders == []
    assert holder_rejected == []
    assert insider_transactions == []
    assert insider_rejected == []
    assert splits is None
    assert dividend_counts == []
    assert dividend_rejected == []
    assert metric_facts == []
    assert esg_activities == []
    assert esg_rejected == []


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
