"""EOD price daily flow.

Ingests end-of-day OHLCV price for all exchange using the provider bulk
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

from core.ingestion import (
    BronzeParseResult,
    LandingObjectRecord,
    PipelineRunScope,
    PipelineRunTracker,
    RejectionRecord,
    RunUnitRecord,
    terminal_status,
)
from core.ingestion.parser import attach_source_uri
from providers.eodhd.models import EODBulkPriceRaw, EODPriceBarRaw

from .models import EODBar
from .parsers import infer_bulk_bar_date, parse_ticker_bars
from .tasks import (
    fetch_eod_price_bulk,
    fetch_eod_provider_exchange_codes,
    fetch_ticker_eod_history,
    load_backfill_pending_symbols,
    parse_eod_price,
    write_backfill_eod_batch,
    write_bronze_eod_price,
    write_eod_price_to_landing,
    write_ticker_eod_history_to_landing,
)

log = structlog.get_logger(__name__)
_REJECTION_SAMPLE_LIMIT_PER_UNIT = 100


@flow(
    name="eod-price-daily",
    description="Ingest EOD OHLCV price for all exchange from the provider bulk endpoint.",
)
async def eod_price_flow(
    trade_date: date | None = None,
    provider_exchange_codes: list[str] | None = None,
) -> dict[str, object]:
    """Ingest EOD price for all (or the given) exchange on trade_date.

    Args:
        trade_date: Specific trading date to ingest. If omitted, the provider returns
            its latest available trading day per exchange.
        provider_exchange_codes: Provider catalog/API codes to ingest. Defaults
            to all provider codes present in bronze.exchange. Pass ["US"] to
            restrict to US equities only.
    """
    codes = provider_exchange_codes or await fetch_eod_provider_exchange_codes()

    total_written = 0
    total_raw = 0
    total_valid = 0
    total_rejected = 0
    summary: dict = {
        "trade_date": trade_date.isoformat() if trade_date else None,
        "exchange": {},
        "failed": [],
    }

    tracker = PipelineRunTracker()
    with tracker.track_run(
        flow_name="eod-price-daily",
        domain="eod_price",
        run_kind="daily" if trade_date is None else "adhoc_date",
        provider="eodhd",
        parameters={
            "trade_date": trade_date.isoformat() if trade_date else None,
            "provider_exchange_codes": provider_exchange_codes,
        },
        target_window_start=trade_date,
        target_window_end=trade_date,
    ) as run:
        log.info("price.flow_start", trade_date=trade_date, exchange=len(codes), run_id=run.run_id)

        try:
            for provider_exchange_code in codes:
                can_mark_failed = True
                try:
                    raw_rows = await fetch_eod_price_bulk(
                        provider_exchange_code=provider_exchange_code,
                        bar_date=trade_date,
                    )
                    total_raw += len(raw_rows)

                    if not raw_rows:
                        log.info(
                            "price.exchange_skipped",
                            provider_exchange_code=provider_exchange_code,
                            reason="no_data",
                            trade_date=trade_date,
                        )
                        summary["exchange"][provider_exchange_code] = {"bar_date": None, "rows_written": 0}
                        can_mark_failed = False
                        run.record_unit(
                            unit_type="exchange_date",
                            unit_key={
                                "provider_exchange_code": provider_exchange_code,
                                "bar_date": trade_date.isoformat() if trade_date else None,
                            },
                            status="skipped",
                            reason="no_data",
                            rows_raw=0,
                            rows_valid=0,
                            rows_rejected=0,
                            rows_written=0,
                        )
                        continue

                    bar_date = trade_date or infer_bulk_bar_date(raw_rows)
                    landing = await write_eod_price_to_landing(
                        raw_rows,
                        provider_exchange_code=provider_exchange_code,
                        bar_date=bar_date,
                    )
                    sources, rejected_rows = parse_eod_price(
                        raw_rows,
                        bar_date=bar_date,
                        provider_exchange_code=provider_exchange_code,
                    )
                    rejected = len(rejected_rows)
                    total_valid += len(sources)
                    total_rejected += rejected
                    bronze = write_bronze_eod_price(
                        sources,
                        provider_exchange_code=provider_exchange_code,
                        bar_date=bar_date,
                        source_uri=landing.source_uri,
                    )

                    summary["exchange"][provider_exchange_code] = {
                        "bar_date": bar_date.isoformat(),
                        "rows_written": bronze.rows_written,
                    }
                    total_written += bronze.rows_written
                    can_mark_failed = False
                    unit_id = run.record_unit_with_landing(
                        landing,
                        unit_type="exchange_date",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "bar_date": bar_date.isoformat(),
                        },
                        status="completed",
                        reason=bronze.reason,
                        rows_raw=len(raw_rows),
                        rows_valid=len(sources),
                        rows_rejected=rejected,
                        rows_written=bronze.rows_written,
                    )
                    run.record_rejections(
                        _daily_rejection_records(
                            run=run,
                            unit_id=unit_id,
                            provider_exchange_code=provider_exchange_code,
                            bar_date=bar_date,
                            source_uri=landing.source_uri,
                            rejected_rows=rejected_rows,
                        )
                    )

                except ValidationError as exc:
                    # Schema drift from provider — re-raise immediately.
                    # All exchange will likely fail the same way; no point continuing.
                    summary["failed"].append(provider_exchange_code)
                    run.record_unit(
                        unit_type="exchange_date",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "bar_date": trade_date.isoformat() if trade_date else None,
                        },
                        status="failed",
                        error=exc,
                        rows_written=0,
                    )
                    run.fail(
                        exc,
                        counters=run.tally.counters(
                            rows_raw=total_raw,
                            rows_valid=total_valid,
                            rows_rejected=total_rejected,
                            rows_written=total_written,
                        ),
                        summary=summary,
                    )
                    raise
                except Exception as exc:
                    if not can_mark_failed:
                        raise
                    log.error("price.exchange_failed", provider_exchange_code=provider_exchange_code, error=str(exc))
                    summary["failed"].append(provider_exchange_code)
                    run.record_unit(
                        unit_type="exchange_date",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "bar_date": trade_date.isoformat() if trade_date else None,
                        },
                        status="failed",
                        error=exc,
                        rows_written=0,
                    )

            run.complete(
                status=terminal_status(
                    failed=run.tally.failed,
                    rejected=total_rejected,
                    skipped_all=_all_units_skipped(total=run.tally.total, skipped=run.tally.skipped),
                ),
                counters=run.tally.counters(
                    rows_raw=total_raw,
                    rows_valid=total_valid,
                    rows_rejected=total_rejected,
                    rows_written=total_written,
                ),
                summary=summary,
            )
            log.info(
                "price.flow_done",
                trade_date=trade_date,
                total_written=total_written,
                failed=len(summary["failed"]),
            )

        except Exception as exc:
            if not run.is_terminal:
                run.fail(
                    exc,
                    counters=run.tally.counters(
                        rows_raw=total_raw,
                        rows_valid=total_valid,
                        rows_rejected=total_rejected,
                        rows_written=total_written,
                    ),
                    summary=summary,
                )
            raise

    return summary


def _daily_rejection_records(
    *,
    run: PipelineRunScope,
    unit_id: str,
    provider_exchange_code: str,
    bar_date: date,
    source_uri: str,
    rejected_rows: list[EODBulkPriceRaw],
) -> list[RejectionRecord]:
    """Build capped daily parser rejection records for one exchange/date unit."""
    return [
        run.rejection_record(
            unit_id=unit_id,
            entity_key={
                "provider_exchange_code": provider_exchange_code,
                "ticker_code": row.code,
                "bar_date": bar_date.isoformat(),
                "raw_date": row.date,
            },
            source_uri=source_uri,
            raw_fragment=row,
            reason="parse_rejected",
        )
        for row in rejected_rows[:_REJECTION_SAMPLE_LIMIT_PER_UNIT]
    ]


def _ticker_rejection_records(
    *,
    run: PipelineRunScope,
    unit_id: str,
    provider_exchange_code: str,
    ticker: str,
    source_uri: str,
    rejected_rows: list[EODPriceBarRaw],
) -> list[RejectionRecord]:
    """Build capped historical parser rejection records for one ticker unit."""
    return [
        run.rejection_record(
            unit_id=unit_id,
            entity_key={
                "provider_exchange_code": provider_exchange_code,
                "ticker": ticker,
                "raw_date": row.date,
            },
            source_uri=source_uri,
            raw_fragment=row,
            reason="parse_rejected",
        )
        for row in rejected_rows[:_REJECTION_SAMPLE_LIMIT_PER_UNIT]
    ]


def _all_units_skipped(*, total: int, skipped: int) -> bool:
    """Return True when the requested scope produced no executable work."""
    return total == 0 or skipped == total


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
    provider_exchange_codes: list[str] | None = None,
    batch_size: int = 50,
) -> dict[str, object]:
    """Ingest full OHLCV history for every instrument in bronze.instrument.

    Processes each exchange sequentially; within an exchange, fetches
    ``batch_size`` symbols concurrently via ``asyncio.gather``. After each
    batch all bars are committed to bronze before the next batch starts,
    giving natural checkpoints for resume on failure.

    A symbol is skipped if it already has any row in bronze.eod_price for
    its provider exchange code — re-runs are safe. To re-ingest a specific
    symbol, pass its provider exchange code and delete its rows from bronze first.

    Provider-validated backfill payloads are written to S3 landing before
    parsing so historical ingestion follows the same replay contract as daily
    and reference flows.

    Args:
        from_date: Earliest bar date to request from the provider.
        to_date: Latest bar date. Defaults to today.
        provider_exchange_codes: Provider catalog/API codes to backfill. Defaults
            to all provider codes present in bronze.exchange.
        batch_size: Symbols fetched concurrently per batch. Keep this low
            enough to stay within the provider's API rate limits.
            At batch_size=50 and ~0.75 s/call the flow can process ~5 k
            symbols/hour, well within the daily quota.
    """
    to_date = to_date or date.today()
    codes = provider_exchange_codes or await fetch_eod_provider_exchange_codes()

    total_written = 0
    total_raw = 0
    total_valid = 0
    total_rejected = 0
    summary: dict = {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "exchange": {},
        "failed_symbols": [],
    }

    tracker = PipelineRunTracker()
    with tracker.track_run(
        flow_name="eod-price-backfill",
        domain="eod_price",
        run_kind="historical_backfill",
        provider="eodhd",
        parameters={
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "provider_exchange_codes": provider_exchange_codes,
            "batch_size": batch_size,
        },
        target_window_start=from_date,
        target_window_end=to_date,
    ) as run:
        log.info(
            "backfill.flow_start",
            from_date=from_date,
            to_date=to_date,
            exchange=len(codes),
            batch_size=batch_size,
            run_id=run.run_id,
        )

        try:
            for provider_exchange_code in codes:
                pending = load_backfill_pending_symbols(provider_exchange_code, from_date)

                if not pending:
                    log.info("backfill.exchange_skip", provider_exchange_code=provider_exchange_code, reason="all_done")
                    summary["exchange"][provider_exchange_code] = {"symbols": 0, "rows": 0}
                    run.record_unit(
                        unit_type="exchange_backfill",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "from_date": from_date.isoformat(),
                            "to_date": to_date.isoformat(),
                        },
                        status="skipped",
                        reason="all_done",
                        rows_written=0,
                    )
                    continue

                log.info("backfill.exchange_start", provider_exchange_code=provider_exchange_code, pending=len(pending))
                exchange_written = 0
                exchange_failed: list[str] = []
                exchange_raw = 0
                exchange_valid = 0
                exchange_rejected = 0

                for i in range(0, len(pending), batch_size):
                    batch_symbols = pending[i : i + batch_size]
                    raw_results = await asyncio.gather(
                        *[fetch_ticker_eod_history(sym, from_date, to_date) for sym in batch_symbols],
                        return_exceptions=True,
                    )

                    batch_sources: list[BronzeParseResult[EODBar]] = []
                    unit_records: list[RunUnitRecord] = []
                    landing_records: list[LandingObjectRecord] = []
                    rejection_records: list[RejectionRecord] = []

                    for sym, result in zip(batch_symbols, raw_results, strict=True):
                        unit_id = str(uuid.uuid4())
                        unit_key: dict[str, object] = {
                            "provider_exchange_code": provider_exchange_code,
                            "ticker": sym,
                            "from_date": from_date.isoformat(),
                            "to_date": to_date.isoformat(),
                        }
                        if isinstance(result, ValidationError):
                            summary["failed_symbols"].append(sym)
                            run.record_unit(
                                unit_id=unit_id,
                                unit_type="ticker_backfill",
                                unit_key=unit_key,
                                status="failed",
                                error=result,
                                rows_written=0,
                            )
                            raise result  # schema drift — abort everything
                        if isinstance(result, BaseException):
                            log.error("backfill.symbol_failed", symbol=sym, error=str(result))
                            exchange_failed.append(sym)
                            unit_records.append(
                                run.unit_record(
                                    unit_id=unit_id,
                                    unit_type="ticker_backfill",
                                    unit_key=unit_key,
                                    status="failed",
                                    error=result,
                                    rows_written=0,
                                )
                            )
                            continue

                        landing = await write_ticker_eod_history_to_landing(
                            result,
                            symbol=sym,
                            provider_exchange_code=provider_exchange_code,
                            from_date=from_date,
                            to_date=to_date,
                        )
                        valid, rejected_rows = parse_ticker_bars(result, ticker=sym)
                        rejected = len(rejected_rows)
                        total_raw += len(result)
                        total_valid += len(valid)
                        total_rejected += rejected
                        exchange_raw += len(result)
                        exchange_valid += len(valid)
                        exchange_rejected += rejected
                        if rejected_rows:
                            log.warning("backfill.parse_rejections", symbol=sym, count=rejected)
                        if valid:
                            batch_sources.extend(attach_source_uri(valid, landing.source_uri))
                        unit_records.append(
                            run.unit_record(
                                unit_id=unit_id,
                                unit_type="ticker_backfill",
                                unit_key=unit_key,
                                status="completed",
                                reason="no_valid_rows" if not valid else None,
                                source_uri=landing.source_uri,
                                rows_raw=len(result),
                                rows_valid=len(valid),
                                rows_rejected=rejected,
                            )
                        )
                        landing_records.append(
                            run.landing_object_record(
                                landing,
                                unit_id=unit_id,
                            )
                        )
                        rejection_records.extend(
                            _ticker_rejection_records(
                                run=run,
                                unit_id=unit_id,
                                provider_exchange_code=provider_exchange_code,
                                ticker=sym,
                                source_uri=landing.source_uri,
                                rejected_rows=rejected_rows,
                            )
                        )

                    batch_written = 0
                    if batch_sources:
                        bronze_batch = write_backfill_eod_batch(
                            batch_sources,
                            provider_exchange_code=provider_exchange_code,
                        )
                        batch_written = bronze_batch.rows_written
                        exchange_written += batch_written

                    run.record_units(unit_records)
                    run.record_landing_objects(landing_records)
                    run.record_rejections(rejection_records)

                    log.info(
                        "backfill.batch_done",
                        provider_exchange_code=provider_exchange_code,
                        batch=i // batch_size + 1,
                        of=(len(pending) + batch_size - 1) // batch_size,
                        rows_written=batch_written,
                        units=len(unit_records),
                    )

                summary["exchange"][provider_exchange_code] = {
                    "symbols_pending": len(pending),
                    "symbols_failed": len(exchange_failed),
                    "rows_written": exchange_written,
                }
                exchange_status = "completed" if not exchange_failed else "failed"
                run.record_unit(
                    unit_type="exchange_backfill",
                    unit_key={
                        "provider_exchange_code": provider_exchange_code,
                        "from_date": from_date.isoformat(),
                        "to_date": to_date.isoformat(),
                    },
                    status=exchange_status,
                    reason="symbol_failures" if exchange_failed else None,
                    rows_raw=exchange_raw,
                    rows_valid=exchange_valid,
                    rows_rejected=exchange_rejected,
                    rows_written=exchange_written,
                )
                summary["failed_symbols"].extend(exchange_failed)
                total_written += exchange_written

            run.complete(
                status=terminal_status(
                    failed=run.tally.failed,
                    rejected=total_rejected,
                    skipped_all=_all_units_skipped(total=run.tally.total, skipped=run.tally.skipped),
                ),
                counters=run.tally.counters(
                    rows_raw=total_raw,
                    rows_valid=total_valid,
                    rows_rejected=total_rejected,
                    rows_written=total_written,
                ),
                summary=summary,
            )
            log.info(
                "backfill.flow_done",
                total_written=total_written,
                failed_symbols=len(summary["failed_symbols"]),
            )

        except Exception as exc:
            if not run.is_terminal:
                run.fail(
                    exc,
                    counters=run.tally.counters(
                        rows_raw=total_raw,
                        rows_valid=total_valid,
                        rows_rejected=total_rejected,
                        rows_written=total_written,
                    ),
                    summary=summary,
                )
            raise

    return summary


if __name__ == "__main__":
    import asyncio

    asyncio.run(eod_price_flow())
