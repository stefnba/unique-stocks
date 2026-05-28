"""Exchange catalog ingestion flow."""

from datetime import date

from prefect import flow

from core.ingestion import PipelineRunTracker, terminal_status
from domains.exchange.tasks import (
    fetch_supported_exchange,
    write_bronze_exchange,
    write_to_landing_zone,
)


@flow(name="exchange-refresh", description="Ingest the list of supported exchange from the configured provider.")
async def exchange_flow() -> int:
    """Fetch the provider exchange catalog and write landing + bronze snapshots."""
    snapshot_date = date.today()
    tracker = PipelineRunTracker()
    with tracker.track_run(
        flow_name="exchange-refresh",
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
            exchange = await fetch_supported_exchange()
            landing = await write_to_landing_zone(exchange=exchange, snapshot_date=snapshot_date)
            bronze = write_bronze_exchange(
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


# ---------------------------------------------------------------------------
# Entry point for manual runs / local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    asyncio.run(exchange_flow())
