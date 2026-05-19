

from prefect import flow

from domains.exchanges.tasks import fetch_supported_exchanges, write_to_landing_zone

@flow(name="exchanges-refresh", description="Ingest the list of supported stock exchanges from EODHD.")
async def exchanges_flow():
    exchanges = await fetch_supported_exchanges()
    write_to_landing_zone(exchanges)



# ---------------------------------------------------------------------------
# Entry point for manual runs / local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    asyncio.run(exchanges_flow())