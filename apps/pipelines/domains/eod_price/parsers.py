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

log = structlog.get_logger(__name__)


def parse_eod_bars(
    raw_rows: list[EODBulkPriceRaw],
    expected_date: date,
    provider_exchange_code: str,
) -> tuple[list[BronzeParseResult[EODBar]], list[EODBulkPriceRaw]]:
    """Validate and parse raw API rows into EODBar domain models.

    The provider exchange code is paired with the row code to form the split
    provider identity stored in Bronze.

    Returns:
        (valid_bars, rejected_rows) — rejected rows are the original raw objects.

    We log rejections but never raise — a few bad instrument rows should not
    abort an entire exchange's worth of data.
    """
    result = parse_best_effort_rows(
        raw_rows,
        lambda row: _build_eod_bar(row, expected_date=expected_date, provider_exchange_code=provider_exchange_code),
        on_rejected=lambda row, exc: log.warning(
            "price.parse_rejected",
            provider_instrument_code=row.code,
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


def parse_instrument_bars(
    raw_bars: list[EODPriceBarRaw],
    provider_exchange_code: str,
    provider_instrument_code: str,
) -> tuple[list[BronzeParseResult[EODBar]], list[EODPriceBarRaw]]:
    """Parse per-instrument historical bars into EODBar domain models.

    Unlike ``parse_eod_bars``, no date-mismatch filtering is applied because
    the provider's per-instrument endpoint returns the requested range.
    """
    result = parse_best_effort_rows(
        raw_bars,
        lambda row: _build_instrument_bar(
            row,
            provider_exchange_code=provider_exchange_code,
            provider_instrument_code=provider_instrument_code,
        ),
        on_rejected=lambda row, exc: log.warning(
            "backfill.parse_rejected",
            provider_exchange_code=provider_exchange_code,
            provider_instrument_code=provider_instrument_code,
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
            "provider_instrument_code": row.code,
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
            provider_exchange_code=provider_exchange_code,
            provider_instrument_code=bar.provider_instrument_code,
            expected=expected_date,
            got=bar.bar_date,
        )
        return None
    return bar


def _build_instrument_bar(
    row: EODPriceBarRaw,
    *,
    provider_exchange_code: str,
    provider_instrument_code: str,
) -> EODBar:
    return EODBar.model_validate(
        {
            "provider_exchange_code": provider_exchange_code,
            "provider_instrument_code": provider_instrument_code,
            "bar_date": parse_date(row.date),
            "open": parse_decimal(row.open),
            "high": parse_decimal(row.high),
            "low": parse_decimal(row.low),
            "close": parse_decimal(row.close),
            "volume": row.volume,
            "adjusted_close": parse_optional_decimal(row.adjusted_close),
        }
    )
