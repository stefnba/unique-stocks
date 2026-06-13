"""Instrument ingestion flow."""

import asyncio
from datetime import date

import structlog
from prefect import flow

from core.ingestion import (
    PipelineRunTracker,
    RunCounters,
    RunStatus,
    RunUnitTally,
    terminal_status,
)
from core.prefect.events import publish_prefect_ingestion_summary
from core.transforms import run_dbt_build_after_ingestion
from domains.instrument.tasks import (
    fetch_instrument,
    fetch_instrument_provider_exchange_codes,
    instrument_already_ingested,
    write_bronze_instrument,
    write_instrument_to_landing_zone,
)

from .assets import record_instrument_bronze_materialization

log = structlog.get_logger(__name__)


@flow(
    name="instrument-refresh",
    description=(
        "Ingest active instruments (equities, ETFs, funds, FX, crypto, etc.) per provider exchange code. "
        "Writes S3 landing + bronze.instrument. Skips exchanges already ingested for snapshot_date."
    ),
)
async def instrument_flow(
    snapshot_date: date | None = None,
    provider_exchange_codes: list[str] | None = None,
    run_dbt_build: bool = False,
) -> dict[str, object]:
    """Ingest active instrument per exchange.

    Fetches all exchange in parallel; writes to S3 and bronze sequentially
    to avoid concurrent DuckDB write conflicts.
    """
    snapshot_date = snapshot_date or date.today()
    codes = provider_exchange_codes or await fetch_instrument_provider_exchange_codes()
    tracker = PipelineRunTracker()
    run_id: str | None = None
    run_status: RunStatus | None = None
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
            "run_dbt_build": run_dbt_build,
        },
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
        run_id = str(run.run_id)
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
                        "instrument.fetch_error",
                        provider_exchange_code=provider_exchange_code,
                        error=str(result),
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
                run.record_unit_with_landing(
                    landing,
                    unit_type="exchange_snapshot",
                    unit_key={
                        "provider_exchange_code": provider_exchange_code,
                        "snapshot_date": snapshot_date.isoformat(),
                    },
                    status="completed",
                    reason=bronze.reason,
                    rows_valid=bronze.rows_written,
                    rows_written=bronze.rows_written,
                )

            log.info(
                "instrument.flow_done",
                snapshot_date=snapshot_date,
                ingested=len(summary["exchange"]),
                skipped=len(summary["skipped"]),
                failed=len(summary["failed"]),
            )
            run_status = terminal_status(
                failed=run.tally.failed,
                skipped_all=len(codes) == 0 or run.tally.skipped == run.tally.total,
            )
            run.complete(
                status=run_status,
                counters=_instrument_counters(tally=run.tally, summary=summary),
                summary=summary,
            )
            rows_written = sum(row.get("rows", 0) for row in summary["exchange"].values())
            if rows_written:
                record_instrument_bronze_materialization(
                    app_run_id=run.run_id,
                    snapshot_date=snapshot_date.isoformat(),
                    provider="eodhd",
                    rows_written=rows_written,
                )
            await publish_prefect_ingestion_summary(
                flow_name="instrument-refresh",
                domain="instrument",
                app_run_id=run.run_id,
                status=run_status,
                summary=summary,
            )
        except Exception as exc:
            if not run.is_terminal:
                run.fail(
                    exc,
                    counters=_instrument_counters(tally=run.tally, summary=summary),
                    summary=summary,
                )
            await publish_prefect_ingestion_summary(
                flow_name="instrument-refresh",
                domain="instrument",
                app_run_id=run.run_id,
                status="failed",
                summary=summary,
            )
            raise
    if run_dbt_build and run_id is not None and run_status is not None:
        summary["dbt_build"] = await run_dbt_build_after_ingestion(
            enabled=run_dbt_build,
            build="instrument-build",
            upstream_status=run_status,
            parent_run_id=run_id,
        )
    return summary


def _instrument_counters(*, tally: RunUnitTally, summary: dict) -> RunCounters:
    """Build aggregate counters for instrument flow audit rows."""
    rows_raw = sum(row.get("raw_rows", 0) for row in summary["exchange"].values())
    rows_written = sum(row.get("rows", 0) for row in summary["exchange"].values())
    return tally.counters(
        rows_raw=rows_raw,
        rows_written=rows_written,
    )
