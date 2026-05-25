"""Exchange catalog ingestion flow."""

from datetime import date

from prefect import flow

from domains.exchange.tasks import (
    fetch_supported_exchange,
    write_bronze_exchange,
    write_to_landing_zone,
)


@flow(name="exchange-refresh", description="Ingest the list of supported exchange from the configured provider.")
async def exchange_flow() -> int:
    """Fetch the provider exchange catalog and write landing + bronze snapshots."""
    snapshot_date = date.today()
    exchange = await fetch_supported_exchange()
    source_uri = await write_to_landing_zone(exchange=exchange, snapshot_date=snapshot_date)
    written = write_bronze_exchange(exchange=exchange, snapshot_date=snapshot_date, source_uri=source_uri)
    return written


# ---------------------------------------------------------------------------
# Entry point for manual runs / local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    asyncio.run(exchange_flow())
