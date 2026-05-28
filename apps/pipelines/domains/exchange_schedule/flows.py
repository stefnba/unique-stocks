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
    description="Ingest trading hours and holiday for all provider-supported exchange.",
)
async def exchange_schedule_flow(
    snapshot_date: date | None = None,
    provider_schedule_exchange_codes: list[str] | None = None,
) -> dict[str, object]:
    """Ingest exchange schedule and holiday for the provider schedule API universe."""
    snapshot_date = snapshot_date or date.today()
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

            # Fetch all exchange concurrently — HTTP is the bottleneck (~0.75 s each).
            # return_exceptions=True prevents one 5xx from cancelling all other tasks.
            results = await asyncio.gather(
                *[fetch_exchange_details(code) for code in pending],
                return_exceptions=True,
            )

            # Write results sequentially to avoid concurrent DuckDB write conflicts.
            for provider_schedule_exchange_code, details in zip(pending, results, strict=True):
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

                landing = await write_schedule_to_landing_zone(details, provider_schedule_exchange_code, snapshot_date)
                schedule_write = write_bronze_exchange_schedule(details, snapshot_date, source_uri=landing.source_uri)
                holiday_write = write_bronze_exchange_holiday(details, snapshot_date, source_uri=landing.source_uri)
                rows_written = schedule_write.rows_written + holiday_write.rows_written
                summary["exchange"][provider_schedule_exchange_code] = {
                    "schedule_rows": schedule_write.rows_written,
                    "holiday_rows": holiday_write.rows_written,
                }
                unit_id = run.record_unit(
                    unit_type="schedule_snapshot",
                    unit_key={
                        "provider_schedule_exchange_code": provider_schedule_exchange_code,
                        "snapshot_date": snapshot_date.isoformat(),
                    },
                    status="completed",
                    reason=_combined_bronze_reason(schedule_write.reason, holiday_write.reason)
                    if rows_written == 0
                    else None,
                    source_uri=landing.source_uri,
                    rows_raw=landing.rows_raw,
                    rows_valid=rows_written,
                    rows_written=rows_written,
                )
                run.record_landing_object(
                    landing,
                    unit_id=unit_id,
                )

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


# ---------------------------------------------------------------------------
# Entry point for manual runs / local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    asyncio.run(exchange_schedule_flow())
