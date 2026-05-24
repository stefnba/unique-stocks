"""EOD price daily flow.

Ingests end-of-day OHLCV price for all exchange using the EODHD bulk
endpoint — one API call per exchange per date, not one per ticker.

Schedule: trigger per exchange after that exchange's market close.
For simplicity, a single daily run at 22:00 UTC catches all exchange
that closed by then (US at ~21:00 UTC, Europe by ~18:00 UTC).
"""

import asyncio
import uuid
from datetime import date

import structlog
from prefect import flow
from pydantic import ValidationError

from core.clients.lake import DataLakeClient, get_lake_client
from core.ingestion import BronzeSource, attach_source_uri
from core.scheduler import last_completed_trading_day

from .models import EODBar
from .parsers import parse_ticker_bars
from .tasks import (
    fetch_eod_exchange_codes,
    fetch_eod_price_bulk,
    fetch_ticker_eod_history,
    load_backfill_pending_symbols,
    parse_eod_price,
    write_backfill_eod_batch,
    write_bronze_eod_price,
    write_eod_price_to_landing,
    write_ticker_eod_history_to_landing,
)

log = structlog.get_logger(__name__)


@flow(
    name="eod-price-daily",
    description="Ingest EOD OHLCV price for all exchange from EODHD bulk endpoint.",
)
async def eod_price_flow(
    trade_date: date | None = None,
    exchange_codes: list[str] | None = None,
) -> dict:
    """Ingest EOD price for all (or the given) exchange on trade_date.

    Args:
        trade_date: The trading date to ingest. Defaults to the last completed
            NYSE trading day. Pass explicitly for backfill or non-US exchange.
        exchange_codes: Exchange to ingest. Defaults to all codes present in
            bronze.exchange (loaded by fetch_eod_exchange_codes). Pass
            ["US"] to restrict to US equities only.
    """
    trade_date = trade_date or last_completed_trading_day()
    codes = exchange_codes or await fetch_eod_exchange_codes()

    lake = get_lake_client()
    run_id = _record_run_start(lake, "eod-price-daily")
    log.info("price.flow_start", trade_date=trade_date, exchange=len(codes), run_id=run_id)

    total_written = 0
    summary: dict = {
        "trade_date": trade_date.isoformat(),
        "exchange": {},
        "failed": [],
    }

    try:
        for exchange in codes:
            try:
                raw_rows = await fetch_eod_price_bulk(exchange=exchange, bar_date=trade_date)

                if not raw_rows:
                    log.info("price.exchange_skipped", exchange=exchange, reason="no_data", trade_date=trade_date)
                    summary["exchange"][exchange] = {"rows_written": 0}
                    continue

                source_uri = await write_eod_price_to_landing(raw_rows, exchange=exchange, bar_date=trade_date)
                sources = parse_eod_price(raw_rows, bar_date=trade_date, exchange=exchange)
                written = write_bronze_eod_price(
                    sources,
                    exchange=exchange,
                    bar_date=trade_date,
                    source_uri=source_uri,
                )

                summary["exchange"][exchange] = {"rows_written": written}
                total_written += written

            except ValidationError:
                # Schema drift from provider — re-raise immediately.
                # All exchange will fail the same way; no point continuing.
                _record_run_failed(lake, run_id, f"ValidationError on exchange {exchange}")
                raise
            except Exception as exc:
                log.error("price.exchange_failed", exchange=exchange, error=str(exc))
                summary["failed"].append(exchange)

        _record_run_complete(lake, run_id, total_written)
        log.info(
            "price.flow_done",
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


@flow(
    name="eod-price-backfill",
    description="Historical EOD backfill for all instrument using the per-ticker endpoint.",
)
async def eod_price_backfill_flow(
    from_date: date,
    to_date: date | None = None,
    exchange_codes: list[str] | None = None,
    batch_size: int = 50,
) -> dict:
    """Ingest full OHLCV history for every instrument in bronze.instrument.

    Processes each exchange sequentially; within an exchange, fetches
    ``batch_size`` symbols concurrently via ``asyncio.gather``. After each
    batch all bars are committed to bronze before the next batch starts,
    giving natural checkpoints for resume on failure.

    A symbol is skipped if it already has any row in bronze.eod_price for
    its exchange — re-runs are safe. To re-ingest a specific symbol, pass it
    via exchange_codes and delete its rows from bronze first.

    Provider-validated backfill payloads are written to S3 landing before
    parsing so historical ingestion follows the same replay contract as daily
    and reference flows.

    Args:
        from_date: Earliest bar date to request from EODHD.
        to_date: Latest bar date. Defaults to today.
        exchange_codes: Exchange to backfill. Defaults to all codes present
            in bronze.exchange.
        batch_size: Symbols fetched concurrently per batch. Keep this low
            enough to stay within EODHD's API rate limits (~100 k calls/day).
            At batch_size=50 and ~0.75 s/call the flow can process ~5 k
            symbols/hour, well within the daily quota.
    """
    to_date = to_date or date.today()
    codes = exchange_codes or await fetch_eod_exchange_codes()

    lake = get_lake_client()
    run_id = _record_run_start(lake, "eod-price-backfill")
    log.info(
        "backfill.flow_start",
        from_date=from_date,
        to_date=to_date,
        exchange=len(codes),
        batch_size=batch_size,
        run_id=run_id,
    )

    total_written = 0
    summary: dict = {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "exchange": {},
        "failed_symbols": [],
    }

    try:
        for exchange_code in codes:
            pending = load_backfill_pending_symbols(exchange_code, from_date)

            if not pending:
                log.info("backfill.exchange_skip", exchange=exchange_code, reason="all_done")
                summary["exchange"][exchange_code] = {"symbols": 0, "rows": 0}
                continue

            log.info("backfill.exchange_start", exchange=exchange_code, pending=len(pending))
            exchange_written = 0
            exchange_failed: list[str] = []

            for i in range(0, len(pending), batch_size):
                batch_symbols = pending[i : i + batch_size]

                raw_results = await asyncio.gather(
                    *[fetch_ticker_eod_history(sym, from_date, to_date) for sym in batch_symbols],
                    return_exceptions=True,
                )

                batch_sources: list[BronzeSource[EODBar]] = []
                for sym, result in zip(batch_symbols, raw_results, strict=True):
                    if isinstance(result, ValidationError):
                        raise result  # schema drift — abort everything
                    if isinstance(result, BaseException):
                        log.error("backfill.symbol_failed", symbol=sym, error=str(result))
                        exchange_failed.append(sym)
                        continue

                    source_uri = await write_ticker_eod_history_to_landing(
                        result,
                        symbol=sym,
                        exchange_code=exchange_code,
                        from_date=from_date,
                        to_date=to_date,
                    )
                    valid, rejected = parse_ticker_bars(result, ticker=sym)
                    if rejected:
                        log.warning("backfill.parse_rejections", symbol=sym, count=len(rejected))
                    if valid:
                        batch_sources.extend(attach_source_uri(valid, source_uri))

                if batch_sources:
                    n = write_backfill_eod_batch(batch_sources, exchange_code=exchange_code)
                    exchange_written += n

                log.info(
                    "backfill.batch_done",
                    exchange=exchange_code,
                    batch=i // batch_size + 1,
                    of=(len(pending) + batch_size - 1) // batch_size,
                )

            summary["exchange"][exchange_code] = {
                "symbols_pending": len(pending),
                "symbols_failed": len(exchange_failed),
                "rows_written": exchange_written,
            }
            summary["failed_symbols"].extend(exchange_failed)
            total_written += exchange_written

        _record_run_complete(lake, run_id, total_written)
        log.info(
            "backfill.flow_done",
            total_written=total_written,
            failed_symbols=len(summary["failed_symbols"]),
        )

    except Exception as exc:
        _record_run_failed(lake, run_id, str(exc))
        raise

    return summary


if __name__ == "__main__":
    import asyncio

    asyncio.run(eod_price_flow())
