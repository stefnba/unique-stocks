"""
Pure transform functions for EOD price data.

No Prefect decorators, no I/O. Fully unit-testable.
"""

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog

from shared.schemas.prices import EODBar

log = structlog.get_logger(__name__)


def parse_eod_bars(raw_rows: list[dict], expected_date: date) -> tuple[list[EODBar], list[dict]]:
    """
    Validate and parse raw API rows into EODBar models.

    Returns:
        (valid_bars, rejected_rows) — rejected rows include an 'error' key.

    We log rejections but never raise — a few bad tickers should not abort
    an entire exchange's worth of data.
    """
    valid: list[EODBar] = []
    rejected: list[dict] = []

    for row in raw_rows:
        try:
            bar = EODBar.model_validate(
                {
                    "ticker": row["ticker"],
                    # EODBar is strict=True — must pass a date object, not a string
                    "bar_date": _to_date(row["bar_date"]),
                    "open": _to_decimal(row.get("open")),
                    "high": _to_decimal(row.get("high")),
                    "low": _to_decimal(row.get("low")),
                    "close": _to_decimal(row.get("close")),
                    "volume": int(row.get("volume") or 0),
                    "adjusted_close": _to_decimal_optional(row.get("adjusted_close")),
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
            rejected.append({**row, "error": str(exc)})
            log.warning("prices.parse_rejected", ticker=row.get("ticker"), error=str(exc))

    return valid, rejected


def bars_to_bronze_records(bars: list[EODBar], provider: str = "eodhd") -> list[dict]:
    """Convert validated EODBar objects to dicts ready for bronze.eod_prices insert."""
    return [bar.to_bronze_record(provider=provider) for bar in bars]


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
