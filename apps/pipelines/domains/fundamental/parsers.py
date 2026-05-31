"""Pure parsers for EODHD fundamentals payloads."""

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from hashlib import sha256
from typing import Any

import structlog

from core.ingestion import BronzeParseResult
from core.ingestion.parser import parse_date, parse_decimal, parse_result
from core.ingestion.serialization import canonical_json
from domains.eod_price.symbols import exchange_from_qualified_ticker, ticker_without_exchange
from domains.fundamental.models import (
    FundamentalDocument,
    FundamentalEtfHolding,
    FundamentalEtfIdentitySnapshot,
    FundamentalFundMetricFact,
    FundamentalIndexComponent,
    FundamentalIndexHistoricalComponent,
    FundamentalIndexIdentitySnapshot,
    FundamentalMutualFundHolding,
    FundamentalMutualFundIdentitySnapshot,
    FundamentalStatementFact,
    FundamentalStockDividendCount,
    FundamentalStockEarningsFact,
    FundamentalStockEsgActivity,
    FundamentalStockHolder,
    FundamentalStockIdentitySnapshot,
    FundamentalStockInsiderTransaction,
    FundamentalStockMetricFact,
    FundamentalStockOutstandingShares,
    FundamentalStockSharesStatsSnapshot,
    FundamentalStockSplitsDividendsSnapshot,
)
from providers.eodhd.models import FundamentalRaw

log = structlog.get_logger(__name__)

_STATEMENT_TYPES = {
    "Balance_Sheet": "balance_sheet",
    "Cash_Flow": "cash_flow",
    "Income_Statement": "income_statement",
}
_PERIOD_TYPES = {
    "yearly": "annual",
    "quarterly": "quarterly",
}
_STATEMENT_METADATA_FIELDS = {"date", "filing_date", "currency_symbol"}
_EARNINGS_TREND_PERIOD_TYPES = {
    "Annual": "annual",
    "Quarterly": "quarterly",
}
_EARNINGS_METADATA_FIELDS = {
    "date",
    "reportDate",
    "beforeAfterMarket",
    "currency",
    "period",
    "type",
    "fiscalQuarter",
}
_OUTSTANDING_SHARES_PERIOD_TYPES = {
    "annual": "annual",
    "quarterly": "quarterly",
}
_HOLDER_TYPES = {
    "Institutions": "institution",
    "Funds": "fund",
}
_STOCK_METRIC_GROUPS = (
    ("highlights", "Highlights"),
    ("valuation", "Valuation"),
    ("technicals", "Technicals"),
    ("analyst_ratings", "AnalystRatings"),
    ("esg_scores", "ESGScores"),
)
_STOCK_METRIC_SKIP_FIELDS = {
    "ActivitiesInvolvement",
    "Disclaimer",
    "MostRecentQuarter",
    "RatingDate",
}
_FUND_METRIC_SKIP_FIELDS = {
    "ActivitiesInvolvement",
    "Holdings",
    "Holdings_Count",
    "Top_10_Holdings",
    "Top_Holdings",
}
_FUND_METRIC_IDENTITY_FIELDS = {
    "AnnualHoldingsTurnover",
    "Average_Mkt_Cap_Mil",
    "Date_Ongoing_Charge",
    "Expense_Ratio",
    "Expense_Ratio_Date",
    "Max_Annual_Mgmt_Charge",
    "Morning_Star_Rating",
    "Morning_Star_Risk_Rating",
    "Nav",
    "NetExpenseRatio",
    "Ongoing_Charge",
    "Portfolio_Net_Assets",
    "Prev_Close_Price",
    "Share_Class_Net_Assets",
    "TotalAssets",
    "Update_Date",
    "Yield",
    "Yield_1Year_YTD",
    "Yield_3Year_YTD",
    "Yield_5Year_YTD",
    "Yield_YTD",
}


def parse_fundamental_document(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> BronzeParseResult[FundamentalDocument]:
    """Parse document-level metadata for a fundamentals payload."""
    general = raw.general
    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    row = FundamentalDocument(
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        ticker=ticker,
        code=_optional_str(general.get("Code")) or ticker_without_exchange(ticker, provider_exchange_code),
        name=_optional_str(general.get("Name")),
        instrument_type=instrument_type,
        instrument_family=instrument_family_from_type(instrument_type),
        primary_ticker=_optional_str(general.get("PrimaryTicker")),
        provider_listing_exchange_code=_optional_str(general.get("Exchange")),
        provider_updated_at=_optional_date(general.get("UpdatedAt")),
        top_level_sections=_top_level_sections(raw),
        payload_hash=_payload_hash(raw),
    )
    return parse_result(row, raw)


def parse_stock_identity_snapshot(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> BronzeParseResult[FundamentalStockIdentitySnapshot] | None:
    """Parse stock-only identity metadata from the ``General`` section."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock":
        return None

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    row = FundamentalStockIdentitySnapshot(
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        ticker=ticker,
        code=_optional_str(general.get("Code")) or ticker_without_exchange(ticker, provider_exchange_code),
        name=_optional_str(general.get("Name")),
        primary_ticker=_optional_str(general.get("PrimaryTicker")),
        provider_listing_exchange_code=_optional_str(general.get("Exchange")),
        currency_code=_optional_str(general.get("CurrencyCode")),
        currency_name=_optional_str(general.get("CurrencyName")),
        country_name=_optional_str(general.get("CountryName")),
        country_iso=_optional_str(general.get("CountryISO")),
        isin=_optional_str(general.get("ISIN")),
        cusip=_optional_str(general.get("CUSIP")),
        cik=_optional_str(general.get("CIK")),
        lei=_optional_str(general.get("LEI")),
        open_figi=_optional_str(general.get("OpenFigi")),
        employer_id_number=_optional_str(general.get("EmployerIdNumber")),
        fiscal_year_end=_optional_str(general.get("FiscalYearEnd")),
        ipo_date=_optional_date(general.get("IPODate")),
        sector=_optional_str(general.get("Sector")),
        industry=_optional_str(general.get("Industry")),
        gic_sector=_optional_str(general.get("GicSector")),
        gic_group=_optional_str(general.get("GicGroup")),
        gic_industry=_optional_str(general.get("GicIndustry")),
        gic_sub_industry=_optional_str(general.get("GicSubIndustry")),
        home_category=_optional_str(general.get("HomeCategory")),
        is_delisted=_optional_bool(general.get("IsDelisted")),
        delisted_date=_optional_date(general.get("DelistedDate")),
        full_time_employees=_optional_int(general.get("FullTimeEmployees")),
        web_url=_optional_str(general.get("WebURL")),
        logo_url=_optional_str(general.get("LogoURL")),
    )
    return parse_result(row, raw.general)


def parse_stock_statement_facts(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalStatementFact]], list[dict[str, Any]]]:
    """Flatten stock financial statements into long-form numeric facts."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock" or not raw.financials:
        return [], []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalStatementFact]] = []
    rejected: list[dict[str, Any]] = []

    for provider_statement, statement_payload in raw.financials.items():
        statement_type = _STATEMENT_TYPES.get(provider_statement)
        if statement_type is None or not isinstance(statement_payload, Mapping):
            continue

        for provider_period, period_payload in statement_payload.items():
            period_type = _PERIOD_TYPES.get(provider_period)
            if period_type is None or not isinstance(period_payload, Mapping):
                continue

            for period_key, report_payload in period_payload.items():
                if not isinstance(report_payload, Mapping):
                    rejected.append(
                        _statement_rejection(
                            ticker=ticker,
                            statement_type=statement_type,
                            period_type=period_type,
                            period_key=str(period_key),
                            reason="report_not_object",
                            raw_value=report_payload,
                        )
                    )
                    continue

                for metric_name, metric_value in report_payload.items():
                    if metric_name in _STATEMENT_METADATA_FIELDS or metric_value is None or metric_value == "":
                        continue

                    raw_fragment = {
                        "ticker": ticker,
                        "statement_type": statement_type,
                        "period_type": period_type,
                        "period_key": str(period_key),
                        "metric_name": str(metric_name),
                        "metric_value": metric_value,
                    }
                    try:
                        fact = FundamentalStatementFact(
                            snapshot_date=snapshot_date,
                            provider_exchange_code=provider_exchange_code,
                            ticker=ticker,
                            statement_type=statement_type,
                            period_type=period_type,
                            period_end_date=parse_date(report_payload.get("date") or period_key),
                            filing_date=_optional_date(report_payload.get("filing_date")),
                            currency_symbol=_optional_str(report_payload.get("currency_symbol")),
                            metric_name=str(metric_name),
                            metric_value=_parse_statement_decimal(metric_value),
                        )
                    except Exception as exc:
                        log.warning(
                            "fundamental.statement_fact_rejected",
                            ticker=ticker,
                            statement_type=statement_type,
                            period_type=period_type,
                            period_key=str(period_key),
                            metric_name=str(metric_name),
                            error=str(exc),
                        )
                        rejected.append({**raw_fragment, "reason": "parse_error", "error": str(exc)})
                        continue

                    valid.append(parse_result(fact, raw_fragment))

    return valid, rejected


def parse_stock_earnings_facts(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalStockEarningsFact]], list[dict[str, Any]]]:
    """Flatten stock earnings sections into long-form numeric facts."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock" or not raw.earnings:
        return [], []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalStockEarningsFact]] = []
    rejected: list[dict[str, Any]] = []

    _parse_earnings_period_map(
        raw.earnings.get("Annual"),
        ticker=ticker,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        earnings_section="annual",
        period_type="annual",
        valid=valid,
        rejected=rejected,
    )
    _parse_earnings_period_map(
        raw.earnings.get("History"),
        ticker=ticker,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        earnings_section="history",
        period_type=None,
        valid=valid,
        rejected=rejected,
    )

    trend = raw.earnings.get("Trend")
    if isinstance(trend, Mapping):
        for provider_period, period_payload in trend.items():
            period_type = _EARNINGS_TREND_PERIOD_TYPES.get(str(provider_period))
            if period_type is None:
                continue
            _parse_earnings_period_map(
                period_payload,
                ticker=ticker,
                snapshot_date=snapshot_date,
                provider_exchange_code=provider_exchange_code,
                earnings_section="trend",
                period_type=period_type,
                valid=valid,
                rejected=rejected,
            )
    elif trend is not None:
        rejected.append(
            _earnings_rejection(
                ticker=ticker,
                earnings_section="trend",
                period_type=None,
                period_key="Trend",
                reason="section_not_object",
                raw_value=trend,
            )
        )

    return valid, rejected


def parse_stock_shares_stats_snapshot(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> BronzeParseResult[FundamentalStockSharesStatsSnapshot] | None:
    """Parse stock share-statistics snapshot fields."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock" or raw.shares_stats is None:
        return None

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    payload = raw.shares_stats
    row = FundamentalStockSharesStatsSnapshot(
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        ticker=ticker,
        shares_outstanding=_optional_decimal(payload.get("SharesOutstanding")),
        shares_float=_optional_decimal(payload.get("SharesFloat")),
        percent_insiders=_optional_decimal(payload.get("PercentInsiders")),
        percent_institutions=_optional_decimal(payload.get("PercentInstitutions")),
        shares_short=_optional_decimal(payload.get("SharesShort")),
        shares_short_prior_month=_optional_decimal(payload.get("SharesShortPriorMonth")),
        short_ratio=_optional_decimal(payload.get("ShortRatio")),
        short_percent_outstanding=_optional_decimal(payload.get("ShortPercentOutstanding")),
        short_percent_float=_optional_decimal(payload.get("ShortPercentFloat")),
    )
    return parse_result(row, raw.shares_stats)


def parse_stock_outstanding_shares(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalStockOutstandingShares]], list[dict[str, Any]]]:
    """Parse historical stock outstanding-shares rows."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock" or not raw.outstanding_shares:
        return [], []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalStockOutstandingShares]] = []
    rejected: list[dict[str, Any]] = []

    for provider_period, period_payload in raw.outstanding_shares.items():
        period_type = _OUTSTANDING_SHARES_PERIOD_TYPES.get(str(provider_period))
        if period_type is None:
            continue
        _parse_outstanding_shares_period_map(
            period_payload,
            ticker=ticker,
            snapshot_date=snapshot_date,
            provider_exchange_code=provider_exchange_code,
            period_type=period_type,
            valid=valid,
            rejected=rejected,
        )

    return valid, rejected


def parse_stock_holders(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalStockHolder]], list[dict[str, Any]]]:
    """Parse stock holder rows from institution and fund holder maps."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock" or not raw.holders:
        return [], []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalStockHolder]] = []
    rejected: list[dict[str, Any]] = []

    for provider_holder_type, holder_payload in raw.holders.items():
        holder_type = _HOLDER_TYPES.get(str(provider_holder_type))
        if holder_type is None:
            continue
        _parse_holder_map(
            holder_payload,
            ticker=ticker,
            snapshot_date=snapshot_date,
            provider_exchange_code=provider_exchange_code,
            holder_type=holder_type,
            valid=valid,
            rejected=rejected,
        )

    return valid, rejected


def parse_stock_insider_transactions(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalStockInsiderTransaction]], list[dict[str, Any]]]:
    """Parse stock insider transaction rows from fundamentals."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock" or raw.insider_transactions is None:
        return [], []

    if not isinstance(raw.insider_transactions, Mapping):
        return [], [
            _insider_transaction_rejection(
                ticker=ticker,
                period_key="insider_transactions",
                reason="section_not_object",
                raw_value=raw.insider_transactions,
            )
        ]

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalStockInsiderTransaction]] = []
    rejected: list[dict[str, Any]] = []
    for period_key, transaction_payload in raw.insider_transactions.items():
        if not isinstance(transaction_payload, Mapping):
            rejected.append(
                _insider_transaction_rejection(
                    ticker=ticker,
                    period_key=str(period_key),
                    reason="report_not_object",
                    raw_value=transaction_payload,
                )
            )
            continue

        transaction = dict(transaction_payload.items())
        raw_fragment = {
            "ticker": ticker,
            "section": "insider_transactions",
            "period_key": str(period_key),
            "owner_name": transaction.get("ownerName"),
            "transaction_date": transaction.get("transactionDate"),
            "transaction_code": transaction.get("transactionCode"),
        }
        try:
            owner_name = _optional_str(transaction.get("ownerName"))
            if owner_name is None:
                raise ValueError("Missing owner name")
            transaction_code = _optional_str(transaction.get("transactionCode"))
            if transaction_code is None:
                raise ValueError("Missing transaction code")
            row = FundamentalStockInsiderTransaction(
                snapshot_date=snapshot_date,
                provider_exchange_code=provider_exchange_code,
                ticker=ticker,
                provider_position=_optional_int(period_key),
                filing_date=_optional_date(transaction.get("date")),
                owner_cik=_optional_str(transaction.get("ownerCik")),
                owner_name=owner_name,
                transaction_date=parse_date(transaction.get("transactionDate")),
                transaction_code=transaction_code,
                transaction_amount=_optional_decimal(transaction.get("transactionAmount")),
                transaction_price=_optional_decimal(transaction.get("transactionPrice")),
                transaction_acquired_disposed=_optional_str(transaction.get("transactionAcquiredDisposed")),
                post_transaction_amount=_optional_decimal(transaction.get("postTransactionAmount")),
                sec_link=_optional_str(transaction.get("secLink")),
            )
        except Exception as exc:
            log.warning(
                "fundamental.insider_transaction_rejected",
                ticker=ticker,
                period_key=str(period_key),
                error=str(exc),
            )
            rejected.append({**raw_fragment, "reason": "parse_error", "error": str(exc)})
            continue

        valid.append(parse_result(row, raw_fragment))

    return valid, rejected


def parse_stock_splits_dividends_snapshot(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> BronzeParseResult[FundamentalStockSplitsDividendsSnapshot] | None:
    """Parse the stock splits/dividends snapshot fields."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock" or raw.splits_dividends is None:
        return None

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    payload = raw.splits_dividends
    row = FundamentalStockSplitsDividendsSnapshot(
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        ticker=ticker,
        forward_annual_dividend_rate=_optional_decimal(payload.get("ForwardAnnualDividendRate")),
        forward_annual_dividend_yield=_optional_decimal(payload.get("ForwardAnnualDividendYield")),
        payout_ratio=_optional_decimal(payload.get("PayoutRatio")),
        dividend_date=_optional_date(payload.get("DividendDate")),
        ex_dividend_date=_optional_date(payload.get("ExDividendDate")),
        last_split_factor=_optional_str(payload.get("LastSplitFactor")),
        last_split_date=_optional_date(payload.get("LastSplitDate")),
    )
    return parse_result(row, raw.splits_dividends)


def parse_stock_dividend_counts(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalStockDividendCount]], list[dict[str, Any]]]:
    """Parse yearly dividend-count rows from the splits/dividends section."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock" or not raw.splits_dividends:
        return [], []

    counts = raw.splits_dividends.get("NumberDividendsByYear")
    if counts is None:
        return [], []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalStockDividendCount]] = []
    rejected: list[dict[str, Any]] = []
    if not isinstance(counts, Mapping):
        return [], [
            {
                "ticker": ticker,
                "section": "dividend_counts",
                "reason": "section_not_object",
                "raw_value": counts,
            }
        ]

    for period_key, row_payload in counts.items():
        if not isinstance(row_payload, Mapping):
            rejected.append(
                {
                    "ticker": ticker,
                    "section": "dividend_counts",
                    "period_key": str(period_key),
                    "reason": "report_not_object",
                    "raw_value": row_payload,
                }
            )
            continue
        payload = dict(row_payload.items())
        raw_fragment = {
            "ticker": ticker,
            "section": "dividend_counts",
            "period_key": str(period_key),
            "year": payload.get("Year"),
        }
        try:
            row = FundamentalStockDividendCount(
                snapshot_date=snapshot_date,
                provider_exchange_code=provider_exchange_code,
                ticker=ticker,
                year=_required_int(payload.get("Year")),
                dividend_count=_required_int(payload.get("Count")),
            )
        except Exception as exc:
            rejected.append({**raw_fragment, "reason": "parse_error", "error": str(exc)})
            continue
        valid.append(parse_result(row, raw_fragment))

    return valid, rejected


def parse_stock_metric_facts(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> list[BronzeParseResult[FundamentalStockMetricFact]]:
    """Parse compact numeric stock metrics into long-form facts."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock":
        return []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalStockMetricFact]] = []
    raw_by_alias = raw.model_dump(mode="python", by_alias=True, exclude_none=True)
    for metric_group, provider_section in _STOCK_METRIC_GROUPS:
        section_payload = raw_by_alias.get(provider_section)
        if not isinstance(section_payload, Mapping):
            continue

        payload = dict(section_payload.items())
        metric_date = _stock_metric_date(metric_group, payload)
        for metric_name_raw, metric_value in payload.items():
            metric_name = str(metric_name_raw)
            if metric_name in _STOCK_METRIC_SKIP_FIELDS:
                continue
            metric_decimal = _maybe_decimal(metric_value)
            if metric_decimal is None:
                continue
            raw_fragment = {
                "ticker": ticker,
                "section": metric_group,
                "metric_name": metric_name,
                "metric_value": metric_value,
            }
            valid.append(
                parse_result(
                    FundamentalStockMetricFact(
                        snapshot_date=snapshot_date,
                        provider_exchange_code=provider_exchange_code,
                        ticker=ticker,
                        metric_group=metric_group,
                        metric_name=metric_name,
                        metric_value=metric_decimal,
                        metric_date=metric_date,
                    ),
                    raw_fragment,
                )
            )

    return valid


def parse_stock_esg_activities(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalStockEsgActivity]], list[dict[str, Any]]]:
    """Parse ESG activity involvement rows when supplied."""
    general = raw.general
    instrument_type = _optional_str(general.get("Type")) or "Unknown"
    if instrument_family_from_type(instrument_type) != "stock" or not raw.esg_scores:
        return [], []

    activities = raw.esg_scores.get("ActivitiesInvolvement")
    if activities is None:
        return [], []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    rating_date = _optional_date(raw.esg_scores.get("RatingDate"))
    valid: list[BronzeParseResult[FundamentalStockEsgActivity]] = []
    rejected: list[dict[str, Any]] = []
    if not isinstance(activities, Mapping):
        return [], [
            {
                "ticker": ticker,
                "section": "esg_activities",
                "reason": "section_not_object",
                "raw_value": activities,
            }
        ]

    for period_key, activity_payload in activities.items():
        if not isinstance(activity_payload, Mapping):
            rejected.append(
                {
                    "ticker": ticker,
                    "section": "esg_activities",
                    "period_key": str(period_key),
                    "reason": "report_not_object",
                    "raw_value": activity_payload,
                }
            )
            continue
        payload = dict(activity_payload.items())
        activity = _optional_str(payload.get("Activity"))
        if activity is None:
            continue
        row = FundamentalStockEsgActivity(
            snapshot_date=snapshot_date,
            provider_exchange_code=provider_exchange_code,
            ticker=ticker,
            rating_date=rating_date,
            activity=activity,
            involvement=_optional_str(payload.get("Involvement")),
        )
        valid.append(
            parse_result(
                row,
                {
                    "ticker": ticker,
                    "section": "esg_activities",
                    "period_key": str(period_key),
                    "activity": activity,
                },
            )
        )

    return valid, rejected


def parse_etf_identity_snapshot(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> BronzeParseResult[FundamentalEtfIdentitySnapshot] | None:
    """Parse ETF-specific identity metadata."""
    general = raw.general
    if instrument_family_from_type(_optional_str(general.get("Type"))) != "etf" or raw.etf_data is None:
        return None

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    etf = raw.etf_data
    row = FundamentalEtfIdentitySnapshot(
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        ticker=ticker,
        code=_optional_str(general.get("Code")) or ticker_without_exchange(ticker, provider_exchange_code),
        name=_optional_str(general.get("Name")),
        primary_ticker=_optional_str(general.get("PrimaryTicker")),
        provider_listing_exchange_code=_optional_str(general.get("Exchange")),
        currency_code=_optional_str(general.get("CurrencyCode")),
        currency_name=_optional_str(general.get("CurrencyName")),
        country_name=_optional_str(general.get("CountryName")),
        country_iso=_optional_str(general.get("CountryISO")),
        isin=_optional_str(etf.get("ISIN")),
        open_figi=_optional_str(general.get("OpenFigi")),
        company_name=_optional_str(etf.get("Company_Name")),
        company_url=_optional_str(etf.get("Company_URL")),
        etf_url=_optional_str(etf.get("ETF_URL")),
        domicile=_optional_str(etf.get("Domicile")),
        index_name=_optional_str(etf.get("Index_Name")),
        inception_date=_optional_date(etf.get("Inception_Date")),
        dividend_paying_frequency=_optional_str(etf.get("Dividend_Paying_Frequency")),
        holdings_count=_optional_int(etf.get("Holdings_Count")),
    )
    return parse_result(row, etf)


def parse_mutual_fund_identity_snapshot(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> BronzeParseResult[FundamentalMutualFundIdentitySnapshot] | None:
    """Parse mutual-fund-specific identity metadata."""
    general = raw.general
    if instrument_family_from_type(_optional_str(general.get("Type"))) != "fund" or raw.mutual_fund_data is None:
        return None

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    fund = raw.mutual_fund_data
    row = FundamentalMutualFundIdentitySnapshot(
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        ticker=ticker,
        code=_optional_str(general.get("Code")) or ticker_without_exchange(ticker, provider_exchange_code),
        name=_optional_str(general.get("Name")),
        primary_ticker=_optional_str(general.get("PrimaryTicker")),
        provider_listing_exchange_code=_optional_str(general.get("Exchange")),
        currency_code=_optional_str(general.get("CurrencyCode")),
        currency_name=_optional_str(general.get("CurrencyName")),
        country_name=_optional_str(general.get("CountryName")),
        country_iso=_optional_str(general.get("CountryISO")),
        isin=_optional_str(general.get("ISIN")),
        cusip=_optional_str(general.get("CUSIP")),
        open_figi=_optional_str(general.get("OpenFigi")),
        fund_category=_optional_str(fund.get("Fund_Category")) or _optional_str(general.get("Fund_Category")),
        fund_family=_optional_str(general.get("Fund_Family")),
        fund_style=_optional_str(fund.get("Fund_Style")) or _optional_str(general.get("Fund_Style")),
        fiscal_year_end=_optional_str(general.get("Fiscal_Year_End")),
        domicile=_optional_str(fund.get("Domicile")),
        inception_date=_optional_date(fund.get("Inception_Date")),
        update_date=_optional_date(fund.get("Update_Date")),
    )
    return parse_result(row, fund)


def parse_index_identity_snapshot(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> BronzeParseResult[FundamentalIndexIdentitySnapshot] | None:
    """Parse index-specific identity metadata."""
    general = raw.general
    if instrument_family_from_type(_optional_str(general.get("Type"))) != "index":
        return None

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    row = FundamentalIndexIdentitySnapshot(
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        ticker=ticker,
        code=_optional_str(general.get("Code")) or ticker_without_exchange(ticker, provider_exchange_code),
        name=_optional_str(general.get("Name")),
        provider_listing_exchange_code=_optional_str(general.get("Exchange")),
        currency_code=_optional_str(general.get("CurrencyCode")),
        currency_name=_optional_str(general.get("CurrencyName")),
        country_name=_optional_str(general.get("CountryName")),
        country_iso=_optional_str(general.get("CountryISO")),
        open_figi=_optional_str(general.get("OpenFigi")),
        market_cap=_optional_decimal(general.get("MarketCap")),
    )
    return parse_result(row, general)


def parse_etf_holdings(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalEtfHolding]], list[dict[str, Any]]]:
    """Parse ETF holdings from ETF_Data.Holdings or Top_10_Holdings."""
    general = raw.general
    if instrument_family_from_type(_optional_str(general.get("Type"))) != "etf" or not raw.etf_data:
        return [], []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    holdings = raw.etf_data.get("Holdings") or raw.etf_data.get("Top_10_Holdings")
    top_10 = raw.etf_data.get("Top_10_Holdings")
    top_10_symbols = set(top_10) if isinstance(top_10, Mapping) else set()
    valid: list[BronzeParseResult[FundamentalEtfHolding]] = []
    rejected: list[dict[str, Any]] = []
    if holdings is None:
        return [], []
    if not isinstance(holdings, Mapping):
        return [], [
            _family_rejection(
                ticker=ticker,
                section="etf_holdings",
                reason="section_not_object",
                raw_value=holdings,
            )
        ]

    for holding_symbol, holding_payload in holdings.items():
        if not isinstance(holding_payload, Mapping):
            rejected.append(
                _family_rejection(
                    ticker=ticker,
                    section="etf_holdings",
                    reason="report_not_object",
                    raw_value=holding_payload,
                    entity_key=str(holding_symbol),
                )
            )
            continue
        holding = dict(holding_payload.items())
        symbol = str(holding_symbol)
        try:
            row = FundamentalEtfHolding(
                snapshot_date=snapshot_date,
                provider_exchange_code=provider_exchange_code,
                ticker=ticker,
                holding_symbol=symbol,
                holding_code=_optional_str(holding.get("Code")),
                holding_exchange=_optional_str(holding.get("Exchange")),
                holding_name=_optional_str(holding.get("Name")),
                sector=_optional_str(holding.get("Sector")),
                industry=_optional_str(holding.get("Industry")),
                country=_optional_str(holding.get("Country")),
                region=_optional_str(holding.get("Region")),
                assets_percent=_optional_decimal(holding.get("Assets_%")),
                is_top_10=symbol in top_10_symbols,
            )
        except Exception as exc:
            rejected.append(
                _family_rejection(
                    ticker=ticker,
                    section="etf_holdings",
                    reason="parse_error",
                    raw_value=holding,
                    entity_key=symbol,
                    error=str(exc),
                )
            )
            continue
        valid.append(parse_result(row, {"ticker": ticker, "section": "etf_holdings", "holding_symbol": symbol}))

    return valid, rejected


def parse_mutual_fund_holdings(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalMutualFundHolding]], list[dict[str, Any]]]:
    """Parse mutual fund top holdings."""
    general = raw.general
    if instrument_family_from_type(_optional_str(general.get("Type"))) != "fund" or not raw.mutual_fund_data:
        return [], []

    holdings = raw.mutual_fund_data.get("Top_Holdings")
    if holdings is None:
        return [], []
    if not isinstance(holdings, Mapping):
        return [], [
            _family_rejection(
                ticker=ticker,
                section="mutual_fund_holdings",
                reason="section_not_object",
                raw_value=holdings,
            )
        ]

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalMutualFundHolding]] = []
    rejected: list[dict[str, Any]] = []
    for period_key, holding_payload in holdings.items():
        if not isinstance(holding_payload, Mapping):
            rejected.append(
                _family_rejection(
                    ticker=ticker,
                    section="mutual_fund_holdings",
                    reason="report_not_object",
                    raw_value=holding_payload,
                    entity_key=str(period_key),
                )
            )
            continue
        holding = dict(holding_payload.items())
        holding_name = _optional_str(holding.get("Name"))
        if holding_name is None:
            continue
        row = FundamentalMutualFundHolding(
            snapshot_date=snapshot_date,
            provider_exchange_code=provider_exchange_code,
            ticker=ticker,
            provider_position=_optional_int(period_key),
            holding_name=holding_name,
            weight_percent=_percent_decimal(holding.get("Weight")),
        )
        valid.append(
            parse_result(row, {"ticker": ticker, "section": "mutual_fund_holdings", "period_key": str(period_key)})
        )

    return valid, rejected


def parse_fund_metric_facts(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> list[BronzeParseResult[FundamentalFundMetricFact]]:
    """Parse ETF and mutual fund nested numeric metrics."""
    general = raw.general
    family = instrument_family_from_type(_optional_str(general.get("Type")))
    if family == "etf":
        payload = raw.etf_data
    elif family == "fund":
        payload = raw.mutual_fund_data
    else:
        return []
    if not payload:
        return []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalFundMetricFact]] = []
    for group_raw, group_payload in payload.items():
        group = str(group_raw)
        if group in _FUND_METRIC_SKIP_FIELDS:
            continue
        if group in _FUND_METRIC_IDENTITY_FIELDS:
            metric_value = _maybe_decimal(group_payload)
            if metric_value is None:
                continue
            valid.append(
                parse_result(
                    FundamentalFundMetricFact(
                        snapshot_date=snapshot_date,
                        provider_exchange_code=provider_exchange_code,
                        ticker=ticker,
                        instrument_family=family,
                        metric_group="profile",
                        metric_category=None,
                        metric_name=group,
                        metric_value=metric_value,
                        metric_date=_fund_metric_date(group, payload),
                    ),
                    {"ticker": ticker, "section": "fund_metric", "metric_group": "profile", "metric_name": group},
                )
            )
            continue

        for category, metric_name, metric_value in _walk_numeric_metrics(group_payload):
            valid.append(
                parse_result(
                    FundamentalFundMetricFact(
                        snapshot_date=snapshot_date,
                        provider_exchange_code=provider_exchange_code,
                        ticker=ticker,
                        instrument_family=family,
                        metric_group=group,
                        metric_category=category,
                        metric_name=metric_name,
                        metric_value=metric_value,
                        metric_date=_fund_metric_date(group, payload),
                    ),
                    {
                        "ticker": ticker,
                        "section": "fund_metric",
                        "metric_group": group,
                        "metric_category": category,
                        "metric_name": metric_name,
                    },
                )
            )

    return valid


def parse_index_components(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalIndexComponent]], list[dict[str, Any]]]:
    """Parse current index component rows."""
    general = raw.general
    if instrument_family_from_type(_optional_str(general.get("Type"))) != "index" or not raw.components:
        return [], []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalIndexComponent]] = []
    rejected: list[dict[str, Any]] = []
    for period_key, component_payload in raw.components.items():
        if not isinstance(component_payload, Mapping):
            rejected.append(
                _family_rejection(
                    ticker=ticker,
                    section="index_components",
                    reason="report_not_object",
                    raw_value=component_payload,
                    entity_key=str(period_key),
                )
            )
            continue
        component = dict(component_payload.items())
        code = _optional_str(component.get("Code"))
        if code is None:
            continue
        exchange = _optional_str(component.get("Exchange"))
        row = FundamentalIndexComponent(
            snapshot_date=snapshot_date,
            provider_exchange_code=provider_exchange_code,
            ticker=ticker,
            provider_position=_optional_int(period_key),
            component_code=code,
            component_exchange=exchange,
            component_ticker=f"{code}.{exchange}" if exchange else None,
            component_name=_optional_str(component.get("Name")),
            sector=_optional_str(component.get("Sector")),
            industry=_optional_str(component.get("Industry")),
            weight=_optional_decimal(component.get("Weight")),
        )
        valid.append(
            parse_result(row, {"ticker": ticker, "section": "index_components", "period_key": str(period_key)})
        )

    return valid, rejected


def parse_index_historical_components(
    raw: FundamentalRaw,
    *,
    ticker: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[FundamentalIndexHistoricalComponent]], list[dict[str, Any]]]:
    """Parse historical index constituent membership rows."""
    general = raw.general
    if (
        instrument_family_from_type(_optional_str(general.get("Type"))) != "index"
        or not raw.historical_ticker_components
    ):
        return [], []

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    valid: list[BronzeParseResult[FundamentalIndexHistoricalComponent]] = []
    rejected: list[dict[str, Any]] = []
    for period_key, component_payload in raw.historical_ticker_components.items():
        if not isinstance(component_payload, Mapping):
            rejected.append(
                _family_rejection(
                    ticker=ticker,
                    section="index_historical_components",
                    reason="report_not_object",
                    raw_value=component_payload,
                    entity_key=str(period_key),
                )
            )
            continue
        component = dict(component_payload.items())
        code = _optional_str(component.get("Code"))
        if code is None:
            continue
        row = FundamentalIndexHistoricalComponent(
            snapshot_date=snapshot_date,
            provider_exchange_code=provider_exchange_code,
            ticker=ticker,
            provider_position=_optional_int(period_key),
            component_code=code,
            component_name=_optional_str(component.get("Name")),
            start_date=_optional_date(component.get("StartDate")),
            end_date=_optional_date(component.get("EndDate")),
            is_active_now=_optional_bool(component.get("IsActiveNow")),
            is_delisted=_optional_bool(component.get("IsDelisted")),
        )
        valid.append(
            parse_result(
                row,
                {"ticker": ticker, "section": "index_historical_components", "period_key": str(period_key)},
            )
        )

    return valid, rejected


def instrument_family_from_type(provider_type: str | None) -> str:
    """Map provider instrument type text into a stable broad family."""
    normalized = (provider_type or "").strip().lower()
    if not normalized:
        return "unknown"
    if "etf" in normalized:
        return "etf"
    if "fund" in normalized:
        return "fund"
    if "index" in normalized:
        return "index"
    if "stock" in normalized or "equity" in normalized:
        return "stock"
    if "bond" in normalized:
        return "bond"
    return "unknown"


def _top_level_sections(raw: FundamentalRaw) -> list[str]:
    payload = raw.model_dump(mode="json", by_alias=True, exclude_none=True)
    return sorted(payload)


def _payload_hash(raw: FundamentalRaw) -> str:
    return sha256(canonical_json(raw).encode()).hexdigest()


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    return parse_date(value)


def _optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"Cannot convert {value!r} to bool")


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(str(value))


def _required_int(value: object) -> int:
    if value is None or value == "":
        raise ValueError("Expected an integer value")
    return int(str(value))


def _parse_statement_decimal(value: object) -> Decimal:
    parsed = parse_decimal(value)
    return parsed


def _optional_decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    return parse_decimal(value)


def _maybe_decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return parse_decimal(value)
    except ValueError:
        return None


def _percent_decimal(value: object) -> Decimal | None:
    if isinstance(value, str) and value.strip().endswith("%"):
        return _maybe_decimal(value.strip()[:-1])
    return _maybe_decimal(value)


def _walk_numeric_metrics(payload: object, path: tuple[str, ...] = ()) -> list[tuple[str | None, str, Decimal]]:
    if isinstance(payload, Mapping):
        metrics: list[tuple[str | None, str, Decimal]] = []
        for key, value in payload.items():
            metrics.extend(_walk_numeric_metrics(value, (*path, str(key))))
        return metrics
    if isinstance(payload, list | tuple):
        metrics = []
        for index, value in enumerate(payload):
            metrics.extend(_walk_numeric_metrics(value, (*path, str(index))))
        return metrics

    value = _maybe_decimal(payload)
    if value is None or not path:
        return []
    category = ".".join(path[:-1]) or None
    return [(category, path[-1], value)]


def _fund_metric_date(metric_name: str, payload: Mapping[str, Any]) -> date | None:
    date_field_by_metric = {
        "Expense_Ratio": "Expense_Ratio_Date",
        "Max_Annual_Mgmt_Charge": "Date_Ongoing_Charge",
        "NetExpenseRatio": "Date_Ongoing_Charge",
        "Ongoing_Charge": "Date_Ongoing_Charge",
        "Portfolio_Net_Assets": "Update_Date",
        "Prev_Close_Price": "Update_Date",
        "Share_Class_Net_Assets": "Update_Date",
        "TotalAssets": "Update_Date",
    }
    date_field = date_field_by_metric.get(metric_name)
    if date_field is None:
        return None
    return _optional_date(payload.get(date_field))


def _parse_earnings_period_map(
    payload: object,
    *,
    ticker: str,
    snapshot_date: date,
    provider_exchange_code: str,
    earnings_section: str,
    period_type: str | None,
    valid: list[BronzeParseResult[FundamentalStockEarningsFact]],
    rejected: list[dict[str, Any]],
) -> None:
    if payload is None:
        return
    if not isinstance(payload, Mapping):
        rejected.append(
            _earnings_rejection(
                ticker=ticker,
                earnings_section=earnings_section,
                period_type=period_type,
                period_key=earnings_section,
                reason="section_not_object",
                raw_value=payload,
            )
        )
        return

    for period_key, report_payload in payload.items():
        if not isinstance(report_payload, Mapping):
            rejected.append(
                _earnings_rejection(
                    ticker=ticker,
                    earnings_section=earnings_section,
                    period_type=period_type,
                    period_key=str(period_key),
                    reason="report_not_object",
                    raw_value=report_payload,
                )
            )
            continue

        report = dict(report_payload.items())
        for metric_name_raw, metric_value in report.items():
            metric_name = str(metric_name_raw)
            if metric_name in _EARNINGS_METADATA_FIELDS or metric_value is None or metric_value == "":
                continue

            raw_fragment = {
                "ticker": ticker,
                "section": "earnings",
                "earnings_section": earnings_section,
                "period_type": period_type,
                "period_key": str(period_key),
                "metric_name": metric_name,
                "metric_value": metric_value,
            }
            try:
                fact = FundamentalStockEarningsFact(
                    snapshot_date=snapshot_date,
                    provider_exchange_code=provider_exchange_code,
                    ticker=ticker,
                    earnings_section=earnings_section,
                    period_type=period_type,
                    fiscal_period_end=parse_date(report.get("date") or period_key),
                    report_date=_optional_date(report.get("reportDate")),
                    before_after_market=_optional_str(report.get("beforeAfterMarket")),
                    currency_code=_optional_str(report.get("currency")),
                    fiscal_quarter=_optional_str(report.get("fiscalQuarter")),
                    period_offset=_optional_str(report.get("period")),
                    metric_name=metric_name,
                    metric_value=_parse_statement_decimal(metric_value),
                )
            except Exception as exc:
                log.warning(
                    "fundamental.earnings_fact_rejected",
                    ticker=ticker,
                    earnings_section=earnings_section,
                    period_type=period_type,
                    period_key=str(period_key),
                    metric_name=str(metric_name),
                    error=str(exc),
                )
                rejected.append({**raw_fragment, "reason": "parse_error", "error": str(exc)})
                continue

            valid.append(parse_result(fact, raw_fragment))


def _parse_holder_map(
    payload: object,
    *,
    ticker: str,
    snapshot_date: date,
    provider_exchange_code: str,
    holder_type: str,
    valid: list[BronzeParseResult[FundamentalStockHolder]],
    rejected: list[dict[str, Any]],
) -> None:
    if payload is None:
        return
    if not isinstance(payload, Mapping):
        rejected.append(
            _holder_rejection(
                ticker=ticker,
                holder_type=holder_type,
                period_key=holder_type,
                reason="section_not_object",
                raw_value=payload,
            )
        )
        return

    for period_key, holder_payload in payload.items():
        if not isinstance(holder_payload, Mapping):
            rejected.append(
                _holder_rejection(
                    ticker=ticker,
                    holder_type=holder_type,
                    period_key=str(period_key),
                    reason="report_not_object",
                    raw_value=holder_payload,
                )
            )
            continue

        holder = dict(holder_payload.items())
        raw_fragment = {
            "ticker": ticker,
            "section": "holders",
            "holder_type": holder_type,
            "period_key": str(period_key),
            "holder_name": holder.get("name"),
        }
        try:
            holder_name = _optional_str(holder.get("name"))
            if holder_name is None:
                raise ValueError("Missing holder name")
            row = FundamentalStockHolder(
                snapshot_date=snapshot_date,
                provider_exchange_code=provider_exchange_code,
                ticker=ticker,
                holder_type=holder_type,
                provider_position=_optional_int(period_key),
                holder_name=holder_name,
                report_date=parse_date(holder.get("date")),
                total_shares_percent=_optional_decimal(holder.get("totalShares")),
                total_assets_percent=_optional_decimal(holder.get("totalAssets")),
                current_shares=_optional_decimal(holder.get("currentShares")),
                shares_change=_optional_decimal(holder.get("change")),
                shares_change_percent=_optional_decimal(holder.get("change_p")),
            )
        except Exception as exc:
            log.warning(
                "fundamental.holder_rejected",
                ticker=ticker,
                holder_type=holder_type,
                period_key=str(period_key),
                error=str(exc),
            )
            rejected.append({**raw_fragment, "reason": "parse_error", "error": str(exc)})
            continue

        valid.append(parse_result(row, raw_fragment))


def _parse_outstanding_shares_period_map(
    payload: object,
    *,
    ticker: str,
    snapshot_date: date,
    provider_exchange_code: str,
    period_type: str,
    valid: list[BronzeParseResult[FundamentalStockOutstandingShares]],
    rejected: list[dict[str, Any]],
) -> None:
    if payload is None:
        return
    if not isinstance(payload, Mapping):
        rejected.append(
            _shares_rejection(
                ticker=ticker,
                period_type=period_type,
                period_key=period_type,
                reason="section_not_object",
                raw_value=payload,
            )
        )
        return

    for period_key, report_payload in payload.items():
        if not isinstance(report_payload, Mapping):
            rejected.append(
                _shares_rejection(
                    ticker=ticker,
                    period_type=period_type,
                    period_key=str(period_key),
                    reason="report_not_object",
                    raw_value=report_payload,
                )
            )
            continue

        report = dict(report_payload.items())
        raw_fragment = {
            "ticker": ticker,
            "section": "outstanding_shares",
            "period_type": period_type,
            "period_key": str(period_key),
            "provider_period_label": str(report.get("date") or period_key),
        }
        try:
            row = FundamentalStockOutstandingShares(
                snapshot_date=snapshot_date,
                provider_exchange_code=provider_exchange_code,
                ticker=ticker,
                period_type=period_type,
                provider_period_label=str(report.get("date") or period_key),
                period_end_date=_outstanding_shares_period_end(report),
                shares_mln=_optional_decimal(report.get("sharesMln")),
                shares=_optional_decimal(report.get("shares")),
            )
        except Exception as exc:
            log.warning(
                "fundamental.outstanding_shares_rejected",
                ticker=ticker,
                period_type=period_type,
                period_key=str(period_key),
                error=str(exc),
            )
            rejected.append({**raw_fragment, "reason": "parse_error", "error": str(exc)})
            continue

        if row.shares_mln is None and row.shares is None:
            continue
        valid.append(parse_result(row, raw_fragment))


def _outstanding_shares_period_end(report: Mapping[object, object]) -> date:
    formatted = report.get("dateFormatted")
    if formatted:
        return parse_date(formatted)

    label = str(report.get("date") or "")
    if len(label) == 4 and label.isdigit():
        return parse_date(f"{label}-12-31")
    quarter_end_dates = {"Q1": "03-31", "Q2": "06-30", "Q3": "09-30", "Q4": "12-31"}
    if len(label) == 7 and label[4] == "-":
        suffix = quarter_end_dates.get(label[5:])
        if suffix:
            return parse_date(f"{label[:4]}-{suffix}")
    return parse_date(label)


def _stock_metric_date(metric_group: str, payload: Mapping[object, object]) -> date | None:
    if metric_group == "highlights":
        return _optional_date(payload.get("MostRecentQuarter"))
    if metric_group == "esg_scores":
        return _optional_date(payload.get("RatingDate"))
    return None


def _statement_rejection(
    *,
    ticker: str,
    statement_type: str,
    period_type: str,
    period_key: str,
    reason: str,
    raw_value: object,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "statement_type": statement_type,
        "period_type": period_type,
        "period_key": period_key,
        "reason": reason,
        "raw_value": raw_value,
    }


def _earnings_rejection(
    *,
    ticker: str,
    earnings_section: str,
    period_type: str | None,
    period_key: str,
    reason: str,
    raw_value: object,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "section": "earnings",
        "earnings_section": earnings_section,
        "period_type": period_type,
        "period_key": period_key,
        "reason": reason,
        "raw_value": raw_value,
    }


def _shares_rejection(
    *,
    ticker: str,
    period_type: str,
    period_key: str,
    reason: str,
    raw_value: object,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "section": "outstanding_shares",
        "period_type": period_type,
        "period_key": period_key,
        "reason": reason,
        "raw_value": raw_value,
    }


def _insider_transaction_rejection(
    *,
    ticker: str,
    period_key: str,
    reason: str,
    raw_value: object,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "section": "insider_transactions",
        "period_key": period_key,
        "reason": reason,
        "raw_value": raw_value,
    }


def _holder_rejection(
    *,
    ticker: str,
    holder_type: str,
    period_key: str,
    reason: str,
    raw_value: object,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "section": "holders",
        "holder_type": holder_type,
        "period_key": period_key,
        "reason": reason,
        "raw_value": raw_value,
    }


def _family_rejection(
    *,
    ticker: str,
    section: str,
    reason: str,
    raw_value: object,
    entity_key: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    rejection: dict[str, Any] = {
        "ticker": ticker,
        "section": section,
        "reason": reason,
        "raw_value": raw_value,
    }
    if entity_key is not None:
        rejection["entity_key"] = entity_key
    if error is not None:
        rejection["error"] = error
    return rejection
