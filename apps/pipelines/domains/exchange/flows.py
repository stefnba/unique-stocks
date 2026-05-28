"""Exchange catalog ingestion flow."""

from datetime import date

from prefect import flow

from core.ingestion import PipelineRunTracker, RunCounters, terminal_status
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
    description="Ingest the list of supported exchange from the configured provider.",
)
async def exchange_catalog_flow() -> int:
    """Fetch the provider exchange catalog and write landing + bronze snapshots."""
    snapshot_date = date.today()
    tracker = PipelineRunTracker()
    with tracker.track_run(
        flow_name="exchange-catalog-refresh",
        domain="exchange",
        run_kind="snapshot",
        provider="eodhd",
        parameters={"snapshot_date": snapshot_date.isoformat()},
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
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
            unit.complete_with_landing(
                landing=landing,
                reason=bronze.reason,
                rows_valid=bronze.rows_written,
                rows_written=bronze.rows_written,
            )
        run.complete(
            status=terminal_status(failed=run.tally.failed),
            rows_raw=landing.rows_raw,
            rows_valid=bronze.rows_written,
            rows_written=bronze.rows_written,
            summary={"snapshot_date": snapshot_date.isoformat(), "rows_written": bronze.rows_written},
        )
        return bronze.rows_written


@flow(name="exchange-mic-registry-refresh", description="Ingest the ISO 10383 MIC registry CSV.")
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
            raise

    return summary


# ---------------------------------------------------------------------------
# Entry point for manual runs / local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    asyncio.run(exchange_catalog_flow())
    asyncio.run(exchange_mic_registry_flow())
