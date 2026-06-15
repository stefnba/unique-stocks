"""Domain service for exchange reference ingestion."""

from datetime import date

from core.ingestion import PipelineRunTracker, RunCounters, RunStatus, terminal_status
from core.orchestration.events import publish_prefect_ingestion_summary
from domains.exchange.contracts import (
    ExchangeCatalogRefreshRequest,
    ExchangeCatalogRefreshResult,
    ExchangeMicRegistryRefreshRequest,
    ExchangeMicRegistryRefreshResult,
)
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

from .assets import (
    record_exchange_catalog_bronze_materialization,
    record_exchange_mic_registry_bronze_materialization,
)


async def run_exchange_catalog_refresh(
    request: ExchangeCatalogRefreshRequest,
) -> ExchangeCatalogRefreshResult:
    """Fetch the provider exchange catalog and write landing + Bronze snapshots."""
    snapshot_date = request.snapshot_date or date.today()
    tracker = PipelineRunTracker()
    rows_raw = 0
    rows_written = 0
    run_id: str | None = None
    run_status: RunStatus | None = None
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
        run_id = str(run.run_id)
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
            run_status = terminal_status(failed=run.tally.failed)
            run.complete(
                status=run_status,
                rows_raw=rows_raw,
                rows_valid=rows_written,
                rows_written=rows_written,
                summary=summary,
            )
            if rows_written:
                record_exchange_catalog_bronze_materialization(
                    app_run_id=run.run_id,
                    snapshot_date=snapshot_date.isoformat(),
                    provider="eodhd",
                    rows_written=rows_written,
                    source_uri=landing.source_uri,
                )
            await publish_prefect_ingestion_summary(
                flow_name="exchange-catalog-refresh",
                domain="exchange",
                app_run_id=run.run_id,
                status=run_status,
                summary=summary,
            )
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

    if run_id is None or run_status is None:
        msg = "Exchange catalog refresh did not record a run result."
        raise RuntimeError(msg)
    return ExchangeCatalogRefreshResult(
        run_id=run_id,
        status=run_status,
        summary=summary,
        rows_written=rows_written,
    )


async def run_exchange_mic_registry_refresh(
    request: ExchangeMicRegistryRefreshRequest,
) -> ExchangeMicRegistryRefreshResult:
    """Fetch the ISO MIC registry CSV and write landing + Bronze snapshots."""
    snapshot_date = request.snapshot_date or date.today()
    tracker = PipelineRunTracker()
    rows_raw = 0
    rows_valid = 0
    rows_rejected = 0
    rows_written = 0
    run_id: str | None = None
    run_status: RunStatus | None = None
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
        run_id = str(run.run_id)
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

            run_status = terminal_status(failed=run.tally.failed, rejected=rows_rejected)
            run.complete(
                status=run_status,
                counters=RunCounters(
                    rows_raw=rows_raw,
                    rows_valid=rows_valid,
                    rows_rejected=rows_rejected,
                    rows_written=rows_written,
                ),
                summary=summary,
            )
            if rows_written:
                record_exchange_mic_registry_bronze_materialization(
                    app_run_id=run.run_id,
                    snapshot_date=snapshot_date.isoformat(),
                    provider="iso10383",
                    rows_written=rows_written,
                )
            await publish_prefect_ingestion_summary(
                flow_name="exchange-mic-registry-refresh",
                domain="exchange",
                app_run_id=run.run_id,
                status=run_status,
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

    if run_id is None or run_status is None:
        msg = "Exchange MIC registry refresh did not record a run result."
        raise RuntimeError(msg)
    return ExchangeMicRegistryRefreshResult(run_id=run_id, status=run_status, summary=summary)


__all__ = ["run_exchange_catalog_refresh", "run_exchange_mic_registry_refresh"]
