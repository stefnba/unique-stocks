"""Instrument ingestion flow."""

import asyncio
from datetime import date

import structlog
from prefect import flow

from domains.instrument.tasks import (
    fetch_instrument,
    fetch_instrument_provider_exchange_codes,
    instrument_already_ingested,
    write_bronze_instrument,
    write_instrument_to_landing_zone,
)

log = structlog.get_logger(__name__)


@flow(
    name="instrument-refresh",
    description="Ingest active instrument for all EODHD exchange.",
)
async def instrument_flow(
    snapshot_date: date | None = None,
    provider_exchange_codes: list[str] | None = None,
) -> dict:
    """Ingest active instrument per exchange.

    Fetches all exchange in parallel; writes to S3 and bronze sequentially
    to avoid concurrent DuckDB write conflicts.
    """
    snapshot_date = snapshot_date or date.today()
    codes = provider_exchange_codes or await fetch_instrument_provider_exchange_codes()

    summary: dict = {
        "snapshot_date": snapshot_date.isoformat(),
        "exchange": {},
        "skipped": [],
        "failed": [],
    }

    pending = []
    for provider_exchange_code in codes:
        if instrument_already_ingested(provider_exchange_code, snapshot_date):
            summary["skipped"].append(provider_exchange_code)
        else:
            pending.append(provider_exchange_code)

    results = await asyncio.gather(
        *[fetch_instrument(code) for code in pending],
        return_exceptions=True,
    )

    for provider_exchange_code, result in zip(pending, results, strict=True):
        if isinstance(result, BaseException):
            log.error("instrument.fetch_error", provider_exchange_code=provider_exchange_code, error=str(result))
            summary["failed"].append(provider_exchange_code)
            continue

        source_uri = await write_instrument_to_landing_zone(result, provider_exchange_code, snapshot_date)
        rows = write_bronze_instrument(result, provider_exchange_code, snapshot_date, source_uri=source_uri)
        summary["exchange"][provider_exchange_code] = {"rows": rows}

    log.info(
        "instrument.flow_done",
        snapshot_date=snapshot_date,
        ingested=len(summary["exchange"]),
        skipped=len(summary["skipped"]),
        failed=len(summary["failed"]),
    )
    return summary


if __name__ == "__main__":
    asyncio.run(instrument_flow())
