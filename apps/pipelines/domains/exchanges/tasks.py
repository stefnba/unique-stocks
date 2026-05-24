"""Prefect tasks for exchange catalog ingestion."""

from datetime import date

import structlog
from prefect import task

from config.blocks import BlockRegistry
from core.ingestion import already_ingested, save_landing, write_bronze
from domains.exchanges.datasets import EXCHANGES_DATASET, EXCHANGES_LANDING
from domains.exchanges.parsers import parse_exchange_snapshots
from providers.eodhd.models import SupportedExchange

log = structlog.get_logger(__name__)


@task(retries=3, log_prints=True)
async def fetch_supported_exchanges() -> list[SupportedExchange]:
    """Fetch and schema-validate the list of supported exchanges from EODHD."""
    from providers.eodhd.client import EODHDClient

    api_key = await BlockRegistry.EODHD_API_KEY.load_async()
    async with EODHDClient(api_key=api_key.get()) as client:
        exchanges = await client.get_exchanges()

    log.info("exchanges.fetch_done", count=len(exchanges))
    return exchanges


@task()
async def write_to_landing_zone(exchanges: list[SupportedExchange]) -> str:
    """Write supported exchanges to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = save_landing(s3, EXCHANGES_LANDING, EXCHANGES_DATASET.provider, exchanges)
    return ref.uri


@task()
def write_bronze_exchanges(
    exchanges: list[SupportedExchange],
    snapshot_date: date,
    source_uri: str | None = None,
) -> int:
    """Write validated exchanges to bronze.exchanges.

    Skips the insert when a snapshot for this date and provider already exists
    to ensure idempotency on re-runs.
    """
    from core.clients.lake import get_lake_client

    if not exchanges:
        log.info("exchanges.write_skipped", reason="no_data", snapshot_date=snapshot_date)
        return 0

    lake = get_lake_client()
    if already_ingested(lake, EXCHANGES_DATASET, snapshot_date=snapshot_date):
        log.info(
            "exchanges.write_skipped",
            reason="already_ingested",
            snapshot_date=snapshot_date,
        )
        return 0

    sources = parse_exchange_snapshots(exchanges, snapshot_date)
    written = write_bronze(lake, EXCHANGES_DATASET, sources, source_uri=source_uri)
    log.info("exchanges.write_done", snapshot_date=snapshot_date, rows=written)
    return written
