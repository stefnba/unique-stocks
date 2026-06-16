"""Domain service for exchange schedule ingestion."""

import asyncio
from datetime import date

import structlog

from core.ingestion import (
    PipelineRunTracker,
    RunCounters,
    RunStatus,
    RunUnitTally,
    terminal_status,
)
from core.orchestration.events import publish_ingestion_summary
from domains.exchange_schedule.contracts import ExchangeScheduleRefreshRequest, ExchangeScheduleRefreshResult
from domains.exchange_schedule.tasks import (
    fetch_exchange_details,
    fetch_provider_schedule_exchange_codes,
    resolve_operational_schedule_exchange_codes,
    schedule_already_ingested,
    write_bronze_exchange_holiday,
    write_bronze_exchange_schedule,
    write_schedule_to_landing_zone,
)

log = structlog.get_logger(__name__)


async def run_exchange_schedule_refresh(
    request: ExchangeScheduleRefreshRequest,
) -> ExchangeScheduleRefreshResult:
    """Ingest exchange schedule and holiday for the operational provider schedule universe."""
    snapshot_date = request.snapshot_date or date.today()
    fetch_batch_size = max(1, int(request.batch_size))
    fetch_batch_delay = max(0.0, float(request.provider_batch_delay_seconds))
    if request.provider_schedule_exchange_codes is None:
        available_schedule_codes = await fetch_provider_schedule_exchange_codes()
        codes = resolve_operational_schedule_exchange_codes(available_schedule_codes)
        scope = {
            "source": "operational_universe_overlap",
            "available_provider_schedule_codes": len(available_schedule_codes),
            "selected_provider_schedule_codes": len(codes),
        }
    else:
        codes = _normalize_codes(request.provider_schedule_exchange_codes)
        scope = {
            "source": "explicit_parameter",
            "available_provider_schedule_codes": None,
            "selected_provider_schedule_codes": len(codes),
        }

    tracker = PipelineRunTracker()
    run_id: str | None = None
    run_status: RunStatus | None = None
    summary: dict = {
        "snapshot_date": snapshot_date.isoformat(),
        "scope": scope,
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
            "provider_schedule_exchange_codes": request.provider_schedule_exchange_codes,
            "batch_size": fetch_batch_size,
            "provider_batch_delay_seconds": fetch_batch_delay,
        },
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
        run_id = str(run.run_id)
        try:
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
                results = await asyncio.gather(
                    *[fetch_exchange_details(code) for code in batch_codes],
                    return_exceptions=True,
                )

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
            run_status = terminal_status(
                failed=run.tally.failed,
                skipped_all=len(codes) == 0 or run.tally.skipped == run.tally.total,
            )
            run.complete(
                status=run_status,
                counters=_schedule_counters(tally=run.tally, summary=summary),
                summary=summary,
            )
            await publish_ingestion_summary(
                flow_name="exchange-schedule-refresh",
                domain="exchange_schedule",
                app_run_id=run.run_id,
                status=run_status,
                summary=summary,
            )
        except Exception as exc:
            if not run.is_terminal:
                run.fail(
                    exc,
                    counters=_schedule_counters(tally=run.tally, summary=summary),
                    summary=summary,
                )
            await publish_ingestion_summary(
                flow_name="exchange-schedule-refresh",
                domain="exchange_schedule",
                app_run_id=run.run_id,
                status="failed",
                summary=summary,
            )
            raise

    if run_id is None or run_status is None:
        msg = "Exchange schedule refresh did not record a run result."
        raise RuntimeError(msg)
    return ExchangeScheduleRefreshResult(run_id=run_id, status=run_status, summary=summary)


def _normalize_codes(codes: list[str]) -> list[str]:
    """Return stable uppercase provider codes with blanks removed."""
    return sorted({code.strip().upper() for code in codes if code and code.strip()})


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


__all__ = ["run_exchange_schedule_refresh"]
