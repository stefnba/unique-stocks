"""Fundamental ingestion flow — not yet implemented."""

from prefect import flow


@flow(name="fundamental-quarterly", description="Ingest financial statements and ratios from the configured provider.")
async def fundamental_flow() -> dict:
    """Ingest financial statements and ratios from the configured provider."""
    raise NotImplementedError("fundamental domain is not yet implemented")
