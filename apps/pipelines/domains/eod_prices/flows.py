"""
EOD prices flow.

Schedule: daily at 4:30pm ET Mon–Fri (after NYSE close).
Can also be triggered manually with an explicit trade_date for backfill.

The flow uses the EODHD bulk endpoint — one API call per exchange, not one
per ticker. For v1 we start with US (exchange="US") only.
"""

from datetime import date

import structlog
from prefect import flow

from core import lake
from core.scheduler import is_trading_day, last_completed_trading_day

from .tasks import fetch_eod_prices_bulk, parse_eod_prices, write_bronze_eod_prices

log = structlog.get_logger(__name__)

# Exchanges to ingest in v1. Add more here as needed.
V1_EXCHANGES = ["US"]


@flow(
    name="eod-prices-daily",
    description="Ingest EOD OHLCV prices for all US equities from EODHD.",
    # 4:30pm ET = 21:30 UTC (EST) / 20:30 UTC (EDT)
    # Using 21:30 UTC as a safe default — Prefect Cloud handles DST automatically
    # if you set the schedule timezone instead.
)
async def eod_prices_flow(trade_date: date | None = None) -> dict:
    """
    Main EOD prices ingestion flow.

    Args:
        trade_date: The trading date to ingest. Defaults to the last completed
                    trading day. Pass explicitly for backfill.

    Returns:
        Summary dict with rows written per exchange.
    """
    trade_date = trade_date or last_completed_trading_day()

    if not is_trading_day(trade_date):
        log.info("prices.skipped", reason="not_trading_day", trade_date=trade_date)
        return {"skipped": True, "trade_date": trade_date.isoformat()}

    run_id = lake.record_run_start("eod-prices-daily")
    log.info("prices.flow_start", trade_date=trade_date, run_id=run_id)

    total_written = 0
    summary: dict = {"trade_date": trade_date.isoformat(), "exchanges": {}}

    try:
        for exchange in V1_EXCHANGES:
            raw_rows = await fetch_eod_prices_bulk(exchange=exchange, bar_date=trade_date)
            bars = parse_eod_prices(raw_rows, bar_date=trade_date, exchange=exchange)
            written = write_bronze_eod_prices(bars, exchange=exchange, bar_date=trade_date)

            summary["exchanges"][exchange] = {"rows_written": written}
            total_written += written

        lake.record_run_complete(run_id, total_written)
        log.info("prices.flow_done", trade_date=trade_date, total_written=total_written)

    except Exception as exc:
        lake.record_run_failed(run_id, str(exc))
        raise

    return summary


# ---------------------------------------------------------------------------
# Entry point for manual runs / local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    asyncio.run(eod_prices_flow())
