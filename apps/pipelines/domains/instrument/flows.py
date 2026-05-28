"""Instrument ingestion flow."""

import asyncio
from datetime import date

import structlog
from prefect import flow

from core.ingestion import PipelineRunTracker, RunCounters, RunUnitTally, terminal_status
from domains.instrument.tasks import (
    fetch_instrument,
    fetch_instrument_provider_exchange_codes,
    instrument_already_ingested,
    write_bronze_instrument,
    write_instrument_to_landing_zone,
)

log = structlog.get_logger(__name__)


@flow(
    name="instrument-refresh",
    description="Ingest active instrument for all provider exchange codes.",
)
async def instrument_flow(
    snapshot_date: date | None = None,
    provider_exchange_codes: list[str] | None = None,
) -> dict[str, object]:
    """Ingest active instrument per exchange.

    Fetches all exchange in parallel; writes to S3 and bronze sequentially
    to avoid concurrent DuckDB write conflicts.
    """
    snapshot_date = snapshot_date or date.today()
    codes = provider_exchange_codes or await fetch_instrument_provider_exchange_codes()
    tracker = PipelineRunTracker()
    summary: dict = {
        "snapshot_date": snapshot_date.isoformat(),
        "exchange": {},
        "skipped": [],
        "failed": [],
    }

    with tracker.track_run(
        flow_name="instrument-refresh",
        domain="instrument",
        run_kind="snapshot",
        provider="eodhd",
        parameters={
            "snapshot_date": snapshot_date.isoformat(),
            "provider_exchange_codes": provider_exchange_codes,
        },
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
        try:
            pending = []
            for provider_exchange_code in codes:
                if instrument_already_ingested(provider_exchange_code, snapshot_date):
                    summary["skipped"].append(provider_exchange_code)
                    run.record_unit(
                        unit_type="exchange_snapshot",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "snapshot_date": snapshot_date.isoformat(),
                        },
                        status="skipped",
                        reason="already_ingested",
                        rows_written=0,
                    )
                else:
                    pending.append(provider_exchange_code)

            results = await asyncio.gather(
                *[fetch_instrument(code) for code in pending],
                return_exceptions=True,
            )

            for provider_exchange_code, result in zip(pending, results, strict=True):
                if isinstance(result, BaseException):
                    log.error(
                        "instrument.fetch_error", provider_exchange_code=provider_exchange_code, error=str(result)
                    )
                    summary["failed"].append(provider_exchange_code)
                    run.record_unit(
                        unit_type="exchange_snapshot",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "snapshot_date": snapshot_date.isoformat(),
                        },
                        status="failed",
                        error=result,
                        rows_written=0,
                    )
                    continue

                landing = await write_instrument_to_landing_zone(result, provider_exchange_code, snapshot_date)
                bronze = write_bronze_instrument(
                    result,
                    provider_exchange_code,
                    snapshot_date,
                    source_uri=landing.source_uri,
                )
                summary["exchange"][provider_exchange_code] = {
                    "rows": bronze.rows_written,
                    "raw_rows": len(result),
                }
                unit_id = run.record_unit(
                    unit_type="exchange_snapshot",
                    unit_key={
                        "provider_exchange_code": provider_exchange_code,
                        "snapshot_date": snapshot_date.isoformat(),
                    },
                    status="completed",
                    reason=bronze.reason,
                    source_uri=landing.source_uri,
                    rows_raw=landing.rows_raw,
                    rows_valid=bronze.rows_written,
                    rows_written=bronze.rows_written,
                )
                run.record_landing_object(
                    landing,
                    unit_id=unit_id,
                )

            log.info(
                "instrument.flow_done",
                snapshot_date=snapshot_date,
                ingested=len(summary["exchange"]),
                skipped=len(summary["skipped"]),
                failed=len(summary["failed"]),
            )
            run.complete(
                status=terminal_status(
                    failed=run.tally.failed,
                    skipped_all=len(codes) == 0 or run.tally.skipped == run.tally.total,
                ),
                counters=_instrument_counters(tally=run.tally, summary=summary),
                summary=summary,
            )
        except Exception as exc:
            if not run.is_terminal:
                run.fail(exc, counters=_instrument_counters(tally=run.tally, summary=summary), summary=summary)
            raise
    return summary


def _instrument_counters(*, tally: RunUnitTally, summary: dict) -> RunCounters:
    """Build aggregate counters for instrument flow audit rows."""
    rows_raw = sum(row.get("raw_rows", 0) for row in summary["exchange"].values())
    rows_written = sum(row.get("rows", 0) for row in summary["exchange"].values())
    return tally.counters(
        rows_raw=rows_raw,
        rows_written=rows_written,
    )


if __name__ == "__main__":
    asyncio.run(instrument_flow())
