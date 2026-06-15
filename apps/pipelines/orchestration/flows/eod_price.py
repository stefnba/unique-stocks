"""EOD price Prefect flows."""

from datetime import date

from prefect import flow

from domains.eod_price.contracts import EodPriceBackfillRequest, EodPriceDailyRequest
from domains.eod_price.service import run_eod_price_backfill, run_eod_price_daily


@flow(
    name="eod-price-daily",
    description=(
        "Daily EOD OHLCV ingestion via the provider bulk endpoint (one API call per exchange). "
        "Writes S3 landing JSON, then bronze.eod_price. Use deployment backfill for a single past date."
    ),
)
async def eod_price_flow(
    trade_date: date | None = None,
    provider_exchange_codes: list[str] | None = None,
    run_dbt_build: bool = False,
) -> dict[str, object]:
    """Ingest EOD price for all or selected exchanges on one trade date."""
    result = await run_eod_price_daily(
        EodPriceDailyRequest(
            trade_date=trade_date,
            provider_exchange_codes=provider_exchange_codes,
            run_dbt_build=run_dbt_build,
        )
    )
    return result.summary


@flow(
    name="eod-price-backfill",
    description=(
        "Historical EOD backfill via the per-instrument endpoint (one API call per instrument, any date range). "
        "Defaults to full provider history through to_date. Skips instruments already completed for the window."
    ),
)
async def eod_price_backfill_flow(
    from_date: date | None = None,
    to_date: date | None = None,
    provider_exchange_codes: list[str] | None = None,
    batch_size: int = 50,
    max_provider_calls: int | None = None,
    build_selection_views_if_missing: bool = False,
    run_dbt_build: bool = False,
) -> dict[str, object]:
    """Ingest full OHLCV history for every latest EODHD provider instrument."""
    result = await run_eod_price_backfill(
        EodPriceBackfillRequest(
            from_date=from_date,
            to_date=to_date,
            provider_exchange_codes=provider_exchange_codes,
            batch_size=batch_size,
            max_provider_calls=max_provider_calls,
            build_selection_views_if_missing=build_selection_views_if_missing,
            run_dbt_build=run_dbt_build,
        )
    )
    return result.summary


__all__ = ["eod_price_backfill_flow", "eod_price_flow"]
