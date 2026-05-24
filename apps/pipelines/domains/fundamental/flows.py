"""Fundamental ingestion flow — not yet implemented."""

from prefect import flow


@flow(name="fundamental-quarterly", description="Ingest financial statements and ratios from EODHD.")
async def fundamental_flow() -> dict:
    """Ingest financial statements and ratios from EODHD."""
    raise NotImplementedError("fundamental domain is not yet implemented")
