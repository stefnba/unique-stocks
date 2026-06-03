"""Exchange schedule and holiday ingestion flow."""

import asyncio
from datetime import date

import structlog
from prefect import flow

from core.ingestion import PipelineRunTracker, RunCounters, RunUnitTally, terminal_status
from domains.exchange_schedule.tasks import (
    fetch_exchange_details,
    fetch_provider_schedule_exchange_codes,
    schedule_already_ingested,
    write_bronze_exchange_holiday,
    write_bronze_exchange_schedule,
    write_schedule_to_landing_zone,
)

log = structlog.get_logger(__name__)


@flow(
    name="exchange-schedule-refresh",
    description=(
        "Ingest trading hours and holidays for provider schedule API codes. "
        "Writes bronze.exchange_schedule and bronze.exchange_holiday. Skips exchanges already ingested for "
        "snapshot_date. Provider fetches run in bounded batches; landing and Bronze writes stay sequential."
    ),
)
async def exchange_schedule_flow(
    snapshot_date: date | None = None,
    provider_schedule_exchange_codes: list[str] | None = None,
    batch_size: int = 10,
    provider_batch_delay_seconds: float = 0.0,
) -> dict[str, object]:
    """Ingest exchange schedule and holiday for the provider schedule API universe.

    ``batch_size`` caps concurrent provider fetches per batch; landing and Bronze
    writes stay sequential within each batch. ``provider_batch_delay_seconds`` adds
    a pause between fetch batches to reduce rate-limit and overload errors.
    """
    snapshot_date = snapshot_date or date.today()
    fetch_batch_size = max(1, int(batch_size))
    fetch_batch_delay = max(0.0, float(provider_batch_delay_seconds))
    codes = provider_schedule_exchange_codes or await fetch_provider_schedule_exchange_codes()
    tracker = PipelineRunTracker()
    summary: dict = {
        "snapshot_date": snapshot_date.isoformat(),
        "exchange": {},
        "unsupported": [],
        "skipped": [],
        "failed": [],
    }

    with tracker.track_run(
        flow_name="exchange-schedule-refresh",
        domain="exchange_schedule",
        run_kind="snapshot",
        provider="eodhd",
        parameters={
            "snapshot_date": snapshot_date.isoformat(),
            "provider_schedule_exchange_codes": provider_schedule_exchange_codes,
            "batch_size": fetch_batch_size,
            "provider_batch_delay_seconds": fetch_batch_delay,
        },
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
        try:
            # Pre-filter exchange already in bronze for this snapshot.
            pending = []
            for provider_schedule_exchange_code in codes:
                if schedule_already_ingested(provider_schedule_exchange_code, snapshot_date):
                    summary["skipped"].append(provider_schedule_exchange_code)
                    run.record_unit(
                        unit_type="schedule_snapshot",
                        unit_key={
                            "provider_schedule_exchange_code": provider_schedule_exchange_code,
                            "snapshot_date": snapshot_date.isoformat(),
                        },
                        status="skipped",
                        reason="already_ingested",
                        rows_written=0,
                    )
                else:
                    pending.append(provider_schedule_exchange_code)

            log.info(
                "schedule.fetch_batches_start",
                pending=len(pending),
                batch_size=fetch_batch_size,
                provider_batch_delay_seconds=fetch_batch_delay,
            )

            for batch_index in range(0, len(pending), fetch_batch_size):
                batch_codes = pending[batch_index : batch_index + fetch_batch_size]
                # return_exceptions=True prevents one failure from cancelling the batch.
                results = await asyncio.gather(
                    *[fetch_exchange_details(code) for code in batch_codes],
                    return_exceptions=True,
                )

                # Write results sequentially to avoid concurrent DuckDB write conflicts.
                for provider_schedule_exchange_code, details in zip(batch_codes, results, strict=True):
                    if isinstance(details, BaseException):
                        log.error(
                            "schedule.fetch_error",
                            provider_schedule_exchange_code=provider_schedule_exchange_code,
                            error=str(details),
                        )
                        summary["failed"].append(provider_schedule_exchange_code)
                        run.record_unit(
                            unit_type="schedule_snapshot",
                            unit_key={
                                "provider_schedule_exchange_code": provider_schedule_exchange_code,
                                "snapshot_date": snapshot_date.isoformat(),
                            },
                            status="failed",
                            error=details,
                            rows_written=0,
                        )
                        continue
                    if details is None:
                        summary["unsupported"].append(provider_schedule_exchange_code)
                        run.record_unit(
                            unit_type="schedule_snapshot",
                            unit_key={
                                "provider_schedule_exchange_code": provider_schedule_exchange_code,
                                "snapshot_date": snapshot_date.isoformat(),
                            },
                            status="unsupported",
                            reason="provider_404",
                            rows_written=0,
                        )
                        continue

                    landing = await write_schedule_to_landing_zone(
                        details, provider_schedule_exchange_code, snapshot_date
                    )
                    schedule_write = write_bronze_exchange_schedule(
                        details, snapshot_date, source_uri=landing.source_uri
                    )
                    holiday_write = write_bronze_exchange_holiday(details, snapshot_date, source_uri=landing.source_uri)
                    rows_written = schedule_write.rows_written + holiday_write.rows_written
                    summary["exchange"][provider_schedule_exchange_code] = {
                        "schedule_rows": schedule_write.rows_written,
                        "holiday_rows": holiday_write.rows_written,
                    }
                    run.record_unit_with_landing(
                        landing,
                        unit_type="schedule_snapshot",
                        unit_key={
                            "provider_schedule_exchange_code": provider_schedule_exchange_code,
                            "snapshot_date": snapshot_date.isoformat(),
                        },
                        status="completed",
                        reason=_combined_bronze_reason(schedule_write.reason, holiday_write.reason)
                        if rows_written == 0
                        else None,
                        rows_valid=rows_written,
                        rows_written=rows_written,
                    )

                if fetch_batch_delay > 0 and batch_index + fetch_batch_size < len(pending):
                    await asyncio.sleep(fetch_batch_delay)

            log.info(
                "schedule.flow_done",
                snapshot_date=snapshot_date,
                ingested=len(summary["exchange"]),
                unsupported=len(summary["unsupported"]),
                skipped=len(summary["skipped"]),
                failed=len(summary["failed"]),
            )
            run.complete(
                status=terminal_status(
                    failed=run.tally.failed,
                    skipped_all=len(codes) == 0 or run.tally.skipped == run.tally.total,
                ),
                counters=_schedule_counters(tally=run.tally, summary=summary),
                summary=summary,
            )
        except Exception as exc:
            if not run.is_terminal:
                run.fail(exc, counters=_schedule_counters(tally=run.tally, summary=summary), summary=summary)
            raise
    return summary


def _schedule_counters(*, tally: RunUnitTally, summary: dict) -> RunCounters:
    """Build aggregate counters for exchange schedule flow audit rows."""
    rows_written = sum(row.get("schedule_rows", 0) + row.get("holiday_rows", 0) for row in summary["exchange"].values())
    return tally.counters(
        rows_raw=len(summary["exchange"]),
        rows_valid=rows_written,
        rows_written=rows_written,
    )


def _combined_bronze_reason(*reasons: str | None) -> str | None:
    """Return a compact reason when all Bronze writes skipped."""
    distinct = list(dict.fromkeys(reason for reason in reasons if reason))
    return "+".join(distinct) if distinct else None
