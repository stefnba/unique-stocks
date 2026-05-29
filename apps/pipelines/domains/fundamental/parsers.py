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
    FundamentalStatementFact,
    FundamentalStockIdentitySnapshot,
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


def _parse_statement_decimal(value: object) -> Decimal:
    parsed = parse_decimal(value)
    return parsed


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
