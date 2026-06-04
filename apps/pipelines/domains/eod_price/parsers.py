"""Pure parsing functions for EOD price data.

No Prefect decorators, no I/O. Fully unit-testable.
"""

from datetime import date

import structlog

from core.ingestion import BronzeParseResult
from core.ingestion.parser import (
    parse_best_effort_rows,
    parse_date,
    parse_decimal,
    parse_optional_decimal,
)
from providers.eodhd.models import EODBulkPriceRaw, EODPriceBarRaw

from .models import EODBar
from .symbols import exchange_from_qualified_ticker, qualified_ticker

log = structlog.get_logger(__name__)


def parse_eod_bars(
    raw_rows: list[EODBulkPriceRaw],
    expected_date: date,
    provider_exchange_code: str,
) -> tuple[list[BronzeParseResult[EODBar]], list[EODBulkPriceRaw]]:
    """Validate and parse raw API rows into EODBar domain models.

    The provider exchange code is required to construct the fully-qualified ticker symbol
    (e.g. provider returns ``code="AAPL"`` on the US exchange → ``ticker="AAPL.US"``).

    Returns:
        (valid_bars, rejected_rows) — rejected rows are the original raw objects.

    We log rejections but never raise — a few bad tickers should not abort
    an entire exchange's worth of data.
    """
    result = parse_best_effort_rows(
        raw_rows,
        lambda row: _build_eod_bar(row, expected_date=expected_date, provider_exchange_code=provider_exchange_code),
        on_rejected=lambda row, exc: log.warning(
            "price.parse_rejected",
            ticker=row.code,
            provider_exchange_code=provider_exchange_code,
            error=str(exc),
        ),
    )
    return result.valid, result.rejected


def infer_bulk_bar_date(raw_rows: list[EODBulkPriceRaw]) -> date:
    """Infer the exchange bar date from a bulk provider response."""
    if not raw_rows:
        raise ValueError("Cannot infer bar_date from an empty bulk EOD response.")
    return max(parse_date(row.date) for row in raw_rows)


def parse_ticker_bars(
    raw_bars: list[EODPriceBarRaw],
    ticker: str,
) -> tuple[list[BronzeParseResult[EODBar]], list[EODPriceBarRaw]]:
    """Parse per-ticker historical bars into EODBar domain models.

    Unlike ``parse_eod_bars``, the ticker is already fully-qualified (e.g.
    ``AAPL.US``) and no date-mismatch filtering is applied — the per-ticker
    endpoint returns exactly the requested range.
    """
    result = parse_best_effort_rows(
        raw_bars,
        lambda row: _build_ticker_bar(row, ticker=ticker),
        on_rejected=lambda row, exc: log.warning(
            "backfill.parse_rejected",
            ticker=ticker,
            date=row.date,
            error=str(exc),
        ),
    )
    return result.valid, result.rejected


def _build_eod_bar(
    row: EODBulkPriceRaw,
    *,
    expected_date: date,
    provider_exchange_code: str,
) -> EODBar | None:
    bar = EODBar.model_validate(
        {
            "provider_exchange_code": provider_exchange_code,
            "ticker": qualified_ticker(row.code, provider_exchange_code),
            # EODBar is strict=True — must pass a date object, not a string
            "bar_date": parse_date(row.date),
            "open": parse_decimal(row.open),
            "high": parse_decimal(row.high),
            "low": parse_decimal(row.low),
            "close": parse_decimal(row.close),
            "volume": row.volume,
            "adjusted_close": parse_optional_decimal(row.adjusted_close),
        }
    )
    # Silently drop rows where the API returned data for a different date
    # Some providers return the previous close when a market was closed.
    if bar.bar_date != expected_date:
        log.debug(
            "price.date_mismatch",
            ticker=bar.ticker,
            expected=expected_date,
            got=bar.bar_date,
        )
        return None
    return bar


def _build_ticker_bar(row: EODPriceBarRaw, *, ticker: str) -> EODBar:
    return EODBar.model_validate(
        {
            "provider_exchange_code": exchange_from_qualified_ticker(ticker),
            "ticker": ticker,
            "bar_date": parse_date(row.date),
            "open": parse_decimal(row.open),
            "high": parse_decimal(row.high),
            "low": parse_decimal(row.low),
            "close": parse_decimal(row.close),
            "volume": row.volume,
            "adjusted_close": parse_optional_decimal(row.adjusted_close),
        }
    )
