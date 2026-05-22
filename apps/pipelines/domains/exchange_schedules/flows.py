"""Exchange schedules and holidays ingestion flow."""

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
    }

    for exchange_code in codes:
        if schedule_already_ingested(exchange_code, snapshot_date):
            summary["skipped"].append(exchange_code)
            continue

        details = await fetch_exchange_details(exchange_code)
        if details is None:
            summary["unsupported"].append(exchange_code)
            continue

        await write_schedule_to_landing_zone(details, exchange_code)
        schedule_rows = write_bronze_exchange_schedule(details, snapshot_date)
        holiday_rows = write_bronze_exchange_holidays(details, snapshot_date)
        summary["exchanges"][exchange_code] = {
            "schedule_rows": schedule_rows,
            "holiday_rows": holiday_rows,
        }

    log.info(
        "schedules.flow_done",
        snapshot_date=snapshot_date,
        ingested=len(summary["exchanges"]),
        unsupported=len(summary["unsupported"]),
        skipped=len(summary["skipped"]),
    )
    return summary


# ---------------------------------------------------------------------------
# Entry point for manual runs / local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    asyncio.run(exchange_schedules_flow())
