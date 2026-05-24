"""Instrument ingestion flow."""

import asyncio
from datetime import date

import structlog
from prefect import flow

from domains.instruments.tasks import (
    fetch_instrument_exchange_codes,
    fetch_instruments,
    instruments_already_ingested,
    write_bronze_instruments,
    write_instruments_to_landing_zone,
)

log = structlog.get_logger(__name__)


@flow(
    name="instruments-refresh",
    description="Ingest active instruments for all EODHD exchanges.",
)
async def instruments_flow(
    snapshot_date: date | None = None,
    exchange_codes: list[str] | None = None,
) -> dict:
    """Ingest active instruments per exchange.

    Fetches all exchanges in parallel; writes to S3 and bronze sequentially
    to avoid concurrent DuckDB write conflicts.
    """
    snapshot_date = snapshot_date or date.today()
    codes = exchange_codes or await fetch_instrument_exchange_codes()

    summary: dict = {
        "snapshot_date": snapshot_date.isoformat(),
        "exchanges": {},
        "skipped": [],
        "failed": [],
    }

    pending = []
    for code in codes:
        if instruments_already_ingested(code, snapshot_date):
            summary["skipped"].append(code)
        else:
            pending.append(code)

    results = await asyncio.gather(
        *[fetch_instruments(code) for code in pending],
        return_exceptions=True,
    )

    for code, result in zip(pending, results, strict=True):
        if isinstance(result, BaseException):
            log.error("instruments.fetch_error", exchange=code, error=str(result))
            summary["failed"].append(code)
            continue

        source_uri = await write_instruments_to_landing_zone(result, code)
        rows = write_bronze_instruments(result, code, snapshot_date, source_uri=source_uri)
        summary["exchanges"][code] = {"rows": rows}

    log.info(
        "instruments.flow_done",
        snapshot_date=snapshot_date,
        ingested=len(summary["exchanges"]),
        skipped=len(summary["skipped"]),
        failed=len(summary["failed"]),
    )
    return summary


if __name__ == "__main__":
    asyncio.run(instruments_flow())
