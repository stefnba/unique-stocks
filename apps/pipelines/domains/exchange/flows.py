"""Exchange catalog ingestion flow."""

from datetime import date

from prefect import flow

from core.ingestion import PipelineRunTracker, RunCounters, terminal_status
from core.prefect.assets import record_prefect_bronze_materializations
from core.prefect.events import publish_prefect_ingestion_summary
from core.transforms import run_dbt_build_deployment
from domains.exchange.tasks.eodhd import (
    fetch_exchange_catalog,
    write_bronze_exchange_catalog,
    write_exchange_catalog_to_landing_zone,
)
from domains.exchange.tasks.iso10383 import (
    fetch_iso10383_mic_csv,
    load_iso10383_mic_raw_rows,
    parse_iso10383_mic_registry,
    write_bronze_exchange_mic_registry,
    write_mic_registry_to_landing_zone,
)


@flow(
    name="exchange-catalog-refresh",
    description=(
        "Fetch the provider exchange catalog (supported MICs/codes) and write "
        "S3 landing + bronze.exchange_catalog for today's snapshot."
    ),
)
async def exchange_catalog_flow() -> int:
    """Fetch the provider exchange catalog and write landing + bronze snapshots."""
    snapshot_date = date.today()
    tracker = PipelineRunTracker()
    rows_raw = 0
    rows_written = 0
    summary: dict[str, object] = {"snapshot_date": snapshot_date.isoformat()}
    with tracker.track_run(
        flow_name="exchange-catalog-refresh",
        domain="exchange",
        run_kind="snapshot",
        provider="eodhd",
        parameters={"snapshot_date": snapshot_date.isoformat()},
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
        try:
            with run.track_unit(
                unit_type="catalog_snapshot",
                unit_key={"snapshot_date": snapshot_date.isoformat()},
            ) as unit:
                exchange = await fetch_exchange_catalog()
                landing = await write_exchange_catalog_to_landing_zone(exchange=exchange, snapshot_date=snapshot_date)
                bronze = write_bronze_exchange_catalog(
                    exchange=exchange,
                    snapshot_date=snapshot_date,
                    source_uri=landing.source_uri,
                )
                rows_raw = landing.rows_raw or 0
                rows_written = bronze.rows_written
                summary = {"snapshot_date": snapshot_date.isoformat(), "rows_written": rows_written}
                unit.complete_with_landing(
                    landing=landing,
                    reason=bronze.reason,
                    rows_valid=rows_written,
                    rows_written=rows_written,
                )
            status = terminal_status(failed=run.tally.failed)
            run.complete(
                status=status,
                rows_raw=rows_raw,
                rows_valid=rows_written,
                rows_written=rows_written,
                summary=summary,
            )
            if rows_written:
                record_prefect_bronze_materializations(
                    ["exchange_catalog"],
                    metadata={
                        "app_run_id": run.run_id,
                        "snapshot_date": snapshot_date.isoformat(),
                        "provider": "eodhd",
                        "rows_written": rows_written,
                        "source_uri": landing.source_uri,
                    },
                )
            await publish_prefect_ingestion_summary(
                flow_name="exchange-catalog-refresh",
                domain="exchange",
                app_run_id=run.run_id,
                status=status,
                summary=summary,
            )
            return rows_written
        except Exception as exc:
            if not run.is_terminal:
                run.fail(
                    exc,
                    counters=RunCounters(rows_raw=rows_raw, rows_valid=rows_written, rows_written=rows_written),
                    summary=summary,
                )
            await publish_prefect_ingestion_summary(
                flow_name="exchange-catalog-refresh",
                domain="exchange",
                app_run_id=run.run_id,
                status="failed",
                summary=summary,
            )
            raise


@flow(
    name="exchange-mic-registry-refresh",
    description=("Download the ISO 10383 MIC registry CSV and write S3 landing + bronze.exchange_mic_registry."),
)
async def exchange_mic_registry_flow(snapshot_date: date | None = None) -> dict[str, object]:
    """Fetch the ISO MIC registry CSV and write landing + bronze snapshots."""
    snapshot_date = snapshot_date or date.today()
    tracker = PipelineRunTracker()
    rows_raw = 0
    rows_valid = 0
    rows_rejected = 0
    rows_written = 0
    summary: dict[str, object] = {}

    with tracker.track_run(
        flow_name="exchange-mic-registry-refresh",
        domain="exchange",
        run_kind="snapshot",
        provider="iso10383",
        parameters={"snapshot_date": snapshot_date.isoformat()},
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
        try:
            with run.track_unit(
                unit_type="mic_registry_snapshot",
                unit_key={"snapshot_date": snapshot_date.isoformat()},
            ) as unit:
                csv_text = await fetch_iso10383_mic_csv()
                raw_rows = load_iso10383_mic_raw_rows(csv_text)
                landing = await write_mic_registry_to_landing_zone(raw_rows=raw_rows, snapshot_date=snapshot_date)
                valid, rejected = parse_iso10383_mic_registry(raw_rows)
                bronze = write_bronze_exchange_mic_registry(
                    mic_rows=valid,
                    snapshot_date=snapshot_date,
                    source_uri=landing.source_uri,
                )
                rows_raw = len(raw_rows)
                rows_valid = len(valid)
                rows_rejected = len(rejected)
                rows_written = bronze.rows_written
                summary = {
                    "rows_raw": rows_raw,
                    "rows_valid": rows_valid,
                    "rows_rejected": rows_rejected,
                    "rows_written": rows_written,
                }
                unit.complete_with_landing(
                    landing=landing,
                    reason=bronze.reason,
                    rows_valid=rows_valid,
                    rows_rejected=rows_rejected,
                    rows_written=rows_written,
                )
                if rejected:
                    run.record_rejections(
                        [
                            run.rejection_record(
                                unit_id=unit.unit_id,
                                entity_key={"mic": row.get("MIC"), "snapshot_date": snapshot_date.isoformat()},
                                source_uri=landing.source_uri,
                                raw_fragment=row,
                                reason="parse_rejected",
                            )
                            for row in rejected[:100]
                        ]
                    )

            run.complete(
                status=terminal_status(failed=run.tally.failed, rejected=rows_rejected),
                counters=RunCounters(
                    rows_raw=rows_raw,
                    rows_valid=rows_valid,
                    rows_rejected=rows_rejected,
                    rows_written=rows_written,
                ),
                summary=summary,
            )
            status = terminal_status(failed=run.tally.failed, rejected=rows_rejected)
            if rows_written:
                record_prefect_bronze_materializations(
                    ["exchange_mic_registry"],
                    metadata={
                        "app_run_id": run.run_id,
                        "snapshot_date": snapshot_date.isoformat(),
                        "provider": "iso10383",
                        "rows_written": rows_written,
                    },
                )
            await publish_prefect_ingestion_summary(
                flow_name="exchange-mic-registry-refresh",
                domain="exchange",
                app_run_id=run.run_id,
                status=status,
                summary=summary,
            )
        except Exception as exc:
            if not run.is_terminal:
                run.fail(
                    exc,
                    counters=RunCounters(
                        rows_raw=rows_raw,
                        rows_valid=rows_valid,
                        rows_rejected=rows_rejected,
                        rows_written=rows_written,
                    ),
                    summary=summary,
                )
            await publish_prefect_ingestion_summary(
                flow_name="exchange-mic-registry-refresh",
                domain="exchange",
                app_run_id=run.run_id,
                status="failed",
                summary=summary,
            )
            raise

    return summary


@flow(
    name="exchange-reference-refresh",
    description=(
        "Refresh exchange reference inputs in order: provider catalog, ISO MIC registry, "
        "exchange dbt contract, scoped provider schedules, and exchange calendars."
    ),
)
async def exchange_reference_refresh_flow(
    snapshot_date: date | None = None,
    schedule_snapshot_date: date | None = None,
    schedule_batch_size: int = 10,
    provider_batch_delay_seconds: float = 0.0,
    run_dbt_build: bool = True,
) -> dict[str, object]:
    """Run the full exchange reference refresh chain.

    ``run_dbt_build=True`` runs ``dbt-build/exchange-build`` after catalog/MIC
    refresh so the schedule flow can resolve its operational scope from the
    latest provider universe. The schedule flow then runs the same exchange
    build again after clean schedule ingestion so calendars include the new
    trading-hours and holiday rows.
    """
    from domains.exchange_schedule.flows import exchange_schedule_flow

    snapshot_date = snapshot_date or date.today()
    schedule_snapshot_date = schedule_snapshot_date or snapshot_date
    summary: dict[str, object] = {
        "snapshot_date": snapshot_date.isoformat(),
        "schedule_snapshot_date": schedule_snapshot_date.isoformat(),
    }

    summary["exchange_catalog_rows_written"] = await exchange_catalog_flow()
    summary["exchange_mic_registry"] = await exchange_mic_registry_flow(snapshot_date=snapshot_date)

    if run_dbt_build:
        summary["exchange_build_before_schedule"] = await run_dbt_build_deployment(
            build="exchange-build",
            parent_run_id=None,
            tags=["exchange-reference-refresh", "exchange-build", "pre-schedule"],
        )
    else:
        summary["exchange_build_before_schedule"] = {"enabled": False, "triggered": False, "build": "exchange-build"}

    summary["exchange_schedule"] = await exchange_schedule_flow(
        snapshot_date=schedule_snapshot_date,
        batch_size=schedule_batch_size,
        provider_batch_delay_seconds=provider_batch_delay_seconds,
        run_dbt_build=run_dbt_build,
    )
    return summary
