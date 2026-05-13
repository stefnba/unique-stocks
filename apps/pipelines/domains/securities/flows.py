"""Securities ingestion flow — not yet implemented."""

from prefect import flow


@flow(name="securities-weekly", description="Ingest securities listed per exchange from EODHD.")
async def securities_flow() -> dict:
    raise NotImplementedError("securities domain is not yet implemented")
