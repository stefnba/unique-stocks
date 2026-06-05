"""EOD price conventions for ``pipeline.ingestion_coverage``.

Historical EOD backfill coverage is keyed by provider exchange, fully-qualified
ticker, and the exact requested date window. ``from_date = None`` means the
provider start date was intentionally omitted so EODHD returns all available
history through ``to_date``. This keeps a no-data or completed outcome for one
window from suppressing a later wider backfill.
"""

from __future__ import annotations

from datetime import date

from providers.registry import Provider

EOD_PRICE_DOMAIN = "eod_price"
EOD_TICKER_BACKFILL_UNIT_TYPE = "ticker_backfill"
NO_VALID_ROWS_COVERAGE_REASON = "no_valid_rows"
PRICE_ROWS_COMPLETED_COVERAGE_REASON = "price_rows_completed"


def eod_ticker_backfill_unit_key(
    *,
    provider_exchange_code: str,
    ticker: str,
    from_date: date | None,
    to_date: date,
) -> dict[str, object]:
    """Build the exact-window unit key for one per-ticker historical EOD backfill."""
    return {
        "provider_exchange_code": provider_exchange_code,
        "ticker": ticker,
        "from_date": from_date.isoformat() if from_date else None,
        "to_date": to_date.isoformat(),
    }


def eod_provider() -> str:
    """Return the provider id stamped on EOD coverage rows."""
    return str(Provider.EODHD)
