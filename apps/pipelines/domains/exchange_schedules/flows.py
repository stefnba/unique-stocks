"""Exchange schedules and holidays ingestion flow."""

import asyncio
from datetime import date

import structlog
from prefect import flow

from domains.exchange_schedules.tasks import (
    fetch_exchange_details,
    fetch_schedule_exchange_codes,
    schedule_already_ingested,
    write_bronze_exchange_holidays,
    write_bronze_exchange_schedule,
    write_schedule_to_landing_zone,
)

log = structlog.get_logger(__name__)


@flow(
    name="exchange-schedules-refresh",
    description="Ingest trading hours and holidays for all v2-supported EODHD exchanges.",
)
async def exchange_schedules_flow(
    snapshot_date: date | None = None,
    exchange_codes: list[str] | None = None,
) -> dict:
    """Ingest exchange schedules and holidays for the v2 schedule API universe."""
    snapshot_date = snapshot_date or date.today()
    codes = exchange_codes or await fetch_schedule_exchange_codes()

    summary: dict = {
        "snapshot_date": snapshot_date.isoformat(),
        "exchanges": {},
        "unsupported": [],
        "skipped": [],
        "failed": [],
    }

    # Pre-filter exchanges already in bronze for this snapshot.
    pending = []
    for code in codes:
        if schedule_already_ingested(code, snapshot_date):
            summary["skipped"].append(code)
        else:
            pending.append(code)

    # Fetch all exchanges concurrently — HTTP is the bottleneck (~0.75 s each).
    # return_exceptions=True prevents one 5xx from cancelling all other tasks.
    results = await asyncio.gather(
        *[fetch_exchange_details(code) for code in pending],
        return_exceptions=True,
    )

    # Write results sequentially to avoid concurrent DuckDB write conflicts.
    for code, details in zip(pending, results, strict=True):
        if isinstance(details, BaseException):
            log.error("schedules.fetch_error", exchange=code, error=str(details))
            summary["failed"].append(code)
            continue
        if details is None:
            summary["unsupported"].append(code)
            continue

        source_uri = await write_schedule_to_landing_zone(details, code)
        schedule_rows = write_bronze_exchange_schedule(details, snapshot_date, source_uri=source_uri)
        holiday_rows = write_bronze_exchange_holidays(details, snapshot_date, source_uri=source_uri)
        summary["exchanges"][code] = {
            "schedule_rows": schedule_rows,
            "holiday_rows": holiday_rows,
        }

    log.info(
        "schedules.flow_done",
        snapshot_date=snapshot_date,
        ingested=len(summary["exchanges"]),
        unsupported=len(summary["unsupported"]),
        skipped=len(summary["skipped"]),
        failed=len(summary["failed"]),
    )
    return summary


# ---------------------------------------------------------------------------
# Entry point for manual runs / local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    asyncio.run(exchange_schedules_flow())
