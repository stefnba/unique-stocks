"""Pure parsing functions for EOD price data.

No Prefect decorators, no I/O. Fully unit-testable.
"""

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog

from core.ingestion import BronzeParseResult
from core.ingestion.parser import parse_many, parse_result
from providers.eodhd.models import EODBulkPriceRaw, EODPriceBarRaw

from .models import EODBar

log = structlog.get_logger(__name__)


def parse_eod_bars(
    raw_rows: list[EODBulkPriceRaw],
    expected_date: date,
    exchange: str,
) -> tuple[list[BronzeParseResult[EODBar]], list[EODBulkPriceRaw]]:
    """Validate and parse raw API rows into EODBar domain models.

    The exchange is required to construct the fully-qualified ticker symbol
    (e.g. EODHD returns ``code="AAPL"`` on the US exchange → ``ticker="AAPL.US"``).

    Returns:
        (valid_bars, rejected_rows) — rejected rows are the original raw objects.

    We log rejections but never raise — a few bad tickers should not abort
    an entire exchange's worth of data.
    """
    return parse_many(
        raw_rows,
        lambda row: _parse_eod_bar(row, expected_date=expected_date, exchange=exchange),
        on_rejected=lambda row, exc: log.warning(
            "price.parse_rejected",
            ticker=row.code,
            exchange=exchange,
            error=str(exc),
        ),
    )


def parse_ticker_bars(
    raw_bars: list[EODPriceBarRaw],
    ticker: str,
) -> tuple[list[BronzeParseResult[EODBar]], list[EODPriceBarRaw]]:
    """Parse per-ticker historical bars into EODBar domain models.

    Unlike ``parse_eod_bars``, the ticker is already fully-qualified (e.g.
    ``AAPL.US``) and no date-mismatch filtering is applied — the per-ticker
    endpoint returns exactly the requested range.
    """
    return parse_many(
        raw_bars,
        lambda row: _parse_ticker_bar(row, ticker=ticker),
        on_rejected=lambda row, exc: log.warning(
            "backfill.parse_rejected",
            ticker=ticker,
            date=row.date,
            error=str(exc),
        ),
    )


def _parse_eod_bar(
    row: EODBulkPriceRaw,
    *,
    expected_date: date,
    exchange: str,
) -> BronzeParseResult[EODBar] | None:
    bar = EODBar.model_validate(
        {
            "exchange_code": exchange,
            "ticker": f"{row.code}.{exchange}",
            # EODBar is strict=True — must pass a date object, not a string
            "bar_date": _to_date(row.date),
            "open": _to_decimal(row.open),
            "high": _to_decimal(row.high),
            "low": _to_decimal(row.low),
            "close": _to_decimal(row.close),
            "volume": row.volume,
            "adjusted_close": _to_decimal_optional(row.adjusted_close),
        }
    )
    # Silently drop rows where the API returned data for a different date
    # (EODHD sometimes returns the previous close when a market was closed)
    if bar.bar_date != expected_date:
        log.debug(
            "price.date_mismatch",
            ticker=bar.ticker,
            expected=expected_date,
            got=bar.bar_date,
        )
        return None
    return parse_result(bar, row)


def _parse_ticker_bar(row: EODPriceBarRaw, *, ticker: str) -> BronzeParseResult[EODBar]:
    bar = EODBar.model_validate(
        {
            "exchange_code": _exchange_from_ticker(ticker),
            "ticker": ticker,
            "bar_date": _to_date(row.date),
            "open": _to_decimal(row.open),
            "high": _to_decimal(row.high),
            "low": _to_decimal(row.low),
            "close": _to_decimal(row.close),
            "volume": row.volume,
            "adjusted_close": _to_decimal_optional(row.adjusted_close),
        }
    )
    return parse_result(bar, row)


def _to_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Cannot convert {value!r} to date") from exc


def _to_decimal(value: object) -> Decimal:
    if value is None:
        raise ValueError("Expected a numeric value, got None")
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Cannot convert {value!r} to Decimal") from exc


def _to_decimal_optional(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _exchange_from_ticker(ticker: str) -> str:
    """Return the exchange suffix from an EODHD-qualified ticker."""
    if "." not in ticker:
        raise ValueError(f"Expected exchange-qualified ticker, got {ticker!r}")
    return ticker.rsplit(".", maxsplit=1)[1]
