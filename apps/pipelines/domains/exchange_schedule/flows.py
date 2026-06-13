"""Exchange schedule and holiday ingestion flow."""

import asyncio
from datetime import date

import structlog
from prefect import flow

from core.ingestion import PipelineRunTracker, RunCounters, RunStatus, RunUnitTally, terminal_status
from core.prefect.assets import record_prefect_bronze_materializations
from core.prefect.events import publish_prefect_ingestion_summary
from core.transforms import run_dbt_build_after_ingestion
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


@flow(
    name="exchange-schedule-refresh",
    description=(
        "Ingest trading hours and holidays for operational provider schedule API codes. "
        "Writes bronze.exchange_schedule and bronze.exchange_holiday. Skips exchanges already ingested for "
        "snapshot_date. Provider fetches run in bounded batches; landing and Bronze writes stay sequential."
    ),
)
async def exchange_schedule_flow(
    snapshot_date: date | None = None,
    provider_schedule_exchange_codes: list[str] | None = None,
    batch_size: int = 10,
    provider_batch_delay_seconds: float = 0.0,
    run_dbt_build: bool = False,
) -> dict[str, object]:
    """Ingest exchange schedule and holiday for the operational provider schedule universe.

    When ``provider_schedule_exchange_codes`` is omitted, the flow calls the
    provider's live schedule-code list and intersects it with the dbt-built EOD
    price universe and MIC candidates. ``batch_size`` caps concurrent provider
    fetches per batch; landing and Bronze writes stay sequential within each
    batch. ``provider_batch_delay_seconds`` adds a pause between fetch batches
    to reduce rate-limit and overload errors. Set ``run_dbt_build=True`` to
    launch ``dbt-build/exchange-build`` after a clean ingestion audit status.
    """
    snapshot_date = snapshot_date or date.today()
    fetch_batch_size = max(1, int(batch_size))
    fetch_batch_delay = max(0.0, float(provider_batch_delay_seconds))
    if provider_schedule_exchange_codes is None:
        available_schedule_codes = await fetch_provider_schedule_exchange_codes()
        codes = resolve_operational_schedule_exchange_codes(available_schedule_codes)
        scope = {
            "source": "operational_universe_overlap",
            "available_provider_schedule_codes": len(available_schedule_codes),
            "selected_provider_schedule_codes": len(codes),
        }
    else:
        codes = _normalize_codes(provider_schedule_exchange_codes)
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
            "provider_schedule_exchange_codes": provider_schedule_exchange_codes,
            "batch_size": fetch_batch_size,
            "provider_batch_delay_seconds": fetch_batch_delay,
            "run_dbt_build": run_dbt_build,
        },
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
        run_id = str(run.run_id)
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
            run_status = terminal_status(
                failed=run.tally.failed,
                skipped_all=len(codes) == 0 or run.tally.skipped == run.tally.total,
            )
            run.complete(
                status=run_status,
                counters=_schedule_counters(tally=run.tally, summary=summary),
                summary=summary,
            )
            schedule_rows = sum(row.get("schedule_rows", 0) for row in summary["exchange"].values())
            holiday_rows = sum(row.get("holiday_rows", 0) for row in summary["exchange"].values())
            asset_names = []
            if schedule_rows:
                asset_names.append("exchange_schedule")
            if holiday_rows:
                asset_names.append("exchange_holiday")
            if asset_names:
                record_prefect_bronze_materializations(
                    asset_names,
                    metadata={
                        "app_run_id": run.run_id,
                        "snapshot_date": snapshot_date.isoformat(),
                        "provider": "eodhd",
                        "schedule_rows": schedule_rows,
                        "holiday_rows": holiday_rows,
                    },
                )
            await publish_prefect_ingestion_summary(
                flow_name="exchange-schedule-refresh",
                domain="exchange_schedule",
                app_run_id=run.run_id,
                status=run_status,
                summary=summary,
            )
        except Exception as exc:
            if not run.is_terminal:
                run.fail(exc, counters=_schedule_counters(tally=run.tally, summary=summary), summary=summary)
            await publish_prefect_ingestion_summary(
                flow_name="exchange-schedule-refresh",
                domain="exchange_schedule",
                app_run_id=run.run_id,
                status="failed",
                summary=summary,
            )
            raise
    if run_dbt_build and run_id is not None and run_status is not None:
        summary["dbt_build"] = await run_dbt_build_after_ingestion(
            enabled=run_dbt_build,
            build="exchange-build",
            upstream_status=run_status,
            parent_run_id=run_id,
        )
    return summary


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
