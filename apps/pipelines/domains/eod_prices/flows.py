"""EOD prices daily flow.

Ingests end-of-day OHLCV prices for all exchanges using the EODHD bulk
endpoint — one API call per exchange per date, not one per ticker.

Schedule: trigger per exchange after that exchange's market close.
For simplicity, a single daily run at 22:00 UTC catches all exchanges
that closed by then (US at ~21:00 UTC, Europe by ~18:00 UTC).
"""

import uuid
from datetime import date

import structlog
from prefect import flow

from core.clients.lake import DataLakeClient, get_lake_client
from core.scheduler import last_completed_trading_day

from .tasks import (
    fetch_eod_exchange_codes,
    fetch_eod_prices_bulk,
    parse_eod_prices,
    write_bronze_eod_prices,
    write_eod_prices_to_landing,
)

log = structlog.get_logger(__name__)


@flow(
    name="eod-prices-daily",
    description="Ingest EOD OHLCV prices for all exchanges from EODHD bulk endpoint.",
)
async def eod_prices_flow(
    trade_date: date | None = None,
    exchange_codes: list[str] | None = None,
) -> dict:
    """Ingest EOD prices for all (or the given) exchanges on trade_date.

    Args:
        trade_date: The trading date to ingest. Defaults to the last completed
            NYSE trading day. Pass explicitly for backfill or non-US exchanges.
        exchange_codes: Exchanges to ingest. Defaults to all codes present in
            bronze.exchanges (loaded by fetch_eod_exchange_codes). Pass
            ["US"] to restrict to US equities only.
    """
    trade_date = trade_date or last_completed_trading_day()
    codes = exchange_codes or await fetch_eod_exchange_codes()

    lake = get_lake_client()
    run_id = _record_run_start(lake, "eod-prices-daily")
    log.info("prices.flow_start", trade_date=trade_date, exchanges=len(codes), run_id=run_id)

    total_written = 0
    summary: dict = {
        "trade_date": trade_date.isoformat(),
        "exchanges": {},
        "failed": [],
    }

    try:
        for exchange in codes:
            try:
                raw_rows = await fetch_eod_prices_bulk(exchange=exchange, bar_date=trade_date)

                if not raw_rows:
                    log.info("prices.exchange_skipped", exchange=exchange, reason="no_data", trade_date=trade_date)
                    summary["exchanges"][exchange] = {"rows_written": 0}
                    continue

                await write_eod_prices_to_landing(raw_rows, exchange=exchange, bar_date=trade_date)
                bars = parse_eod_prices(raw_rows, bar_date=trade_date, exchange=exchange)
                written = write_bronze_eod_prices(bars, exchange=exchange, bar_date=trade_date)

                summary["exchanges"][exchange] = {"rows_written": written}
                total_written += written

            except Exception as exc:
                log.error("prices.exchange_failed", exchange=exchange, error=str(exc))
                summary["failed"].append(exchange)

        _record_run_complete(lake, run_id, total_written)
        log.info(
            "prices.flow_done",
            trade_date=trade_date,
            total_written=total_written,
            failed=len(summary["failed"]),
        )

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
