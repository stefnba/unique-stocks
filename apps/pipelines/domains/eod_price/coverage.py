"""EOD price conventions for ``pipeline.ingestion_coverage``.

Historical EOD backfill coverage is keyed by provider exchange, fully-qualified
ticker, and the exact requested date range. This keeps a no-data outcome for a
short range from suppressing a later wider backfill.
"""

from __future__ import annotations

from datetime import date

from providers.registry import Provider

EOD_PRICE_DOMAIN = "eod_price"
EOD_TICKER_BACKFILL_UNIT_TYPE = "ticker_backfill"
NO_VALID_ROWS_COVERAGE_REASON = "no_valid_rows"


def eod_ticker_backfill_unit_key(
    *,
    provider_exchange_code: str,
    ticker: str,
    from_date: date,
    to_date: date,
) -> dict[str, object]:
    """Build the exact-range unit key for one per-ticker historical EOD backfill."""
    return {
        "provider_exchange_code": provider_exchange_code,
        "ticker": ticker,
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
    }


def eod_provider() -> str:
    """Return the provider id stamped on EOD coverage rows."""
    return str(Provider.EODHD)
