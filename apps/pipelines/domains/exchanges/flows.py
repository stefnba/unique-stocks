"""Exchange catalog ingestion flow."""

from datetime import date

from prefect import flow

from domains.exchanges.tasks import (
    fetch_supported_exchanges,
    write_bronze_exchanges,
    write_to_landing_zone,
)


@flow(name="exchanges-refresh", description="Ingest the list of supported exchanges from EODHD.")
async def exchanges_flow() -> int:
    """Fetch the EODHD exchange catalog and write landing + bronze snapshots."""
    exchanges = await fetch_supported_exchanges()
    source_uri = await write_to_landing_zone(exchanges)
    written = write_bronze_exchanges(exchanges, snapshot_date=date.today(), source_uri=source_uri)
    return written


# ---------------------------------------------------------------------------
# Entry point for manual runs / local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    asyncio.run(exchanges_flow())
