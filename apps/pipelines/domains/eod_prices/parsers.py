"""Pure parsing functions for EOD price data.

No Prefect decorators, no I/O. Fully unit-testable.
"""

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog

from providers.eodhd.models import EODBulkPriceRaw, EODPriceBarRaw

from .models import EODBar

log = structlog.get_logger(__name__)


def parse_eod_bars(
    raw_rows: list[EODBulkPriceRaw],
    expected_date: date,
    exchange: str,
) -> tuple[list[EODBar], list[EODBulkPriceRaw]]:
    """Validate and parse raw API rows into EODBar domain models.

    The exchange is required to construct the fully-qualified ticker symbol
    (e.g. EODHD returns ``code="AAPL"`` on the US exchange → ``ticker="AAPL.US"``).

    Returns:
        (valid_bars, rejected_rows) — rejected rows are the original raw objects.

    We log rejections but never raise — a few bad tickers should not abort
    an entire exchange's worth of data.
    """
    valid: list[EODBar] = []
    rejected: list[EODBulkPriceRaw] = []

    for row in raw_rows:
        try:
            bar = EODBar.model_validate(
                {
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
                    "prices.date_mismatch",
                    ticker=bar.ticker,
                    expected=expected_date,
                    got=bar.bar_date,
                )
                continue
            valid.append(bar)
        except Exception as exc:
            rejected.append(row)
            log.warning("prices.parse_rejected", ticker=row.code, exchange=exchange, error=str(exc))

    return valid, rejected


def parse_ticker_bars(
    raw_bars: list[EODPriceBarRaw],
    ticker: str,
) -> tuple[list[EODBar], list[EODPriceBarRaw]]:
    """Parse per-ticker historical bars into EODBar domain models.

    Unlike ``parse_eod_bars``, the ticker is already fully-qualified (e.g.
    ``AAPL.US``) and no date-mismatch filtering is applied — the per-ticker
    endpoint returns exactly the requested range.
    """
    valid: list[EODBar] = []
    rejected: list[EODPriceBarRaw] = []

    for row in raw_bars:
        try:
            bar = EODBar.model_validate(
                {
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
            valid.append(bar)
        except Exception as exc:
            rejected.append(row)
            log.warning("backfill.parse_rejected", ticker=ticker, date=row.date, error=str(exc))

    return valid, rejected


def bars_to_bronze_records(
    bars: list[EODBar],
    exchange_code: str,
    provider: str = "eodhd",
) -> list[dict[str, Any]]:
    """Convert validated EODBar objects to dicts ready for bronze.eod_prices insert."""
    return [{**bar.to_bronze_record(provider=provider), "exchange_code": exchange_code} for bar in bars]


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
