"""Fundamentals ingestion flow — not yet implemented."""

from prefect import flow


@flow(name="fundamentals-quarterly", description="Ingest financial statements and ratios from EODHD.")
async def fundamentals_flow() -> dict:
    raise NotImplementedError("fundamentals domain is not yet implemented")
