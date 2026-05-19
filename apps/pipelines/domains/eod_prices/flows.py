"""
EOD prices flow.

Schedule: daily at 4:30pm ET Mon–Fri (after NYSE close).
Can also be triggered manually with an explicit trade_date for backfill.

The flow uses the EODHD bulk endpoint — one API call per exchange, not one
per ticker. For v1 we start with US (exchange="US") only.
"""

import uuid
from datetime import date

import structlog
from prefect import flow

from core.clients.lake import DataLakeClient, get_lake_client
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

    lake = get_lake_client()
    run_id = _record_run_start(lake, "eod-prices-daily")
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

        _record_run_complete(lake, run_id, total_written)
        log.info("prices.flow_done", trade_date=trade_date, total_written=total_written)

    except Exception as exc:
        _record_run_failed(lake, run_id, str(exc))
        raise

    return summary


# ---------------------------------------------------------------------------
# Pipeline run tracking helpers
# ---------------------------------------------------------------------------


def _record_run_start(lake: DataLakeClient, flow_name: str) -> str:
    run_id = str(uuid.uuid4())
    lake.execute(
        "INSERT INTO pipeline.runs (run_id, flow_name, status, started_at) VALUES (?, ?, 'running', now())",
        [run_id, flow_name],
    )
    return run_id


def _record_run_complete(lake: DataLakeClient, run_id: str, rows_written: int) -> None:
    lake.execute(
        "UPDATE pipeline.runs SET status = 'completed', completed_at = now(), rows_written = ? WHERE run_id = ?",
        [rows_written, run_id],
    )


def _record_run_failed(lake: DataLakeClient, run_id: str, error: str) -> None:
    lake.execute(
        "UPDATE pipeline.runs SET status = 'failed', completed_at = now(), error_message = ? WHERE run_id = ?",
        [error[:2000], run_id],
    )


# ---------------------------------------------------------------------------
# Entry point for manual runs / local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    asyncio.run(eod_prices_flow())
