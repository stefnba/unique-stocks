"""Exchanges ingestion flow — not yet implemented."""

from prefect import flow


@flow(name="exchanges-refresh", description="Ingest the list of stock exchanges from EODHD.")
async def exchanges_flow() -> dict:
    raise NotImplementedError("exchanges domain is not yet implemented")
