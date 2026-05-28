"""Prefect tasks for exchange catalog ingestion."""

from datetime import date

import structlog
from prefect import task

from config.blocks import BlockRegistry
from core.ingestion import BronzeWrite, LandingWrite
from domains.exchange.datasets import EXCHANGE_DATASET
from domains.exchange.parsers import parse_exchange_snapshots
from providers.eodhd.models import SupportedExchange

log = structlog.get_logger(__name__)


@task(retries=3, log_prints=True)
async def fetch_supported_exchange() -> list[SupportedExchange]:
    """Fetch and schema-validate the list of supported exchange from the provider."""
    from providers.eodhd.client import EODHDClient

    api_key = await BlockRegistry.EODHD_API_KEY.load_async()
    async with EODHDClient(api_key=api_key.get()) as client:
        exchange = await client.get_exchange()

    log.info("exchange.fetch_done", count=len(exchange))
    return exchange


@task()
async def write_to_landing_zone(
    exchange: list[SupportedExchange],
    snapshot_date: date,
) -> LandingWrite:
    """Write supported exchange to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = EXCHANGE_DATASET.landings.catalog.save(
        s3,
        provider=EXCHANGE_DATASET.provider,
        data=exchange,
        snapshot_date=snapshot_date,
    )
    return EXCHANGE_DATASET.landings.catalog.landing_write(ref, snapshot_date=snapshot_date, rows_raw=len(exchange))


@task()
def write_bronze_exchange(
    exchange: list[SupportedExchange],
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write validated exchange to bronze.exchange.

    Skips the insert when a snapshot for this date and provider already exists
    to ensure idempotency on re-runs.
    """
    from core.clients.lake import get_lake_client

    if not exchange:
        log.info("exchange.write_skipped", reason="no_data", snapshot_date=snapshot_date)
        return BronzeWrite(rows_written=0, reason="no_data")

    lake = get_lake_client()
    if EXCHANGE_DATASET.already_ingested(lake, snapshot_date=snapshot_date):
        log.info(
            "exchange.write_skipped",
            reason="already_ingested",
            snapshot_date=snapshot_date,
        )
        return BronzeWrite(rows_written=0, reason="already_ingested")

    sources = parse_exchange_snapshots(exchange, snapshot_date)
    written = EXCHANGE_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info(
        "exchange.write_done",
        snapshot_date=snapshot_date,
        rows=written,
    )
    return BronzeWrite(rows_written=written)
