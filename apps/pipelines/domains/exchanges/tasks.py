"""Prefect tasks for exchanges ingestion."""

from datetime import date

import structlog
from prefect import task

from config.blocks import BlockRegistry
from core.clients.storage.s3 import S3Key
from providers.eodhd.models import SupportedExchange

log = structlog.get_logger(__name__)


@task(
    retries=3,
    log_prints=True,
)
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
    key = S3Key.snapshot(S3Key.Provider.EODHD, S3Key.Domain.EXCHANGES).jsonl()
    ref = s3.save(key, exchanges)
    return ref.uri


@task()
def write_bronze_exchanges(exchanges: list[SupportedExchange], snapshot_date: date) -> int:
    """Write validated exchanges to bronze.exchanges.

    Skips the insert when a snapshot for this date and provider already exists
    to ensure idempotency on re-runs.
    """
    from core.clients.lake import get_lake_client

    if not exchanges:
        log.info("exchanges.write_skipped", reason="no_data", snapshot_date=snapshot_date)
        return 0

    lake = get_lake_client()
    qualified = lake.qualified_name("bronze", "exchanges")
    already_ingested = lake.query_one(
        f"SELECT COUNT(*) AS cnt FROM {qualified} WHERE snapshot_date = ? AND provider = ?",
        [snapshot_date.isoformat(), SupportedExchange.provider],
    )
    if already_ingested and already_ingested["cnt"] > 0:
        log.info(
            "exchanges.write_skipped",
            reason="already_ingested",
            snapshot_date=snapshot_date,
        )
        return 0

    records = [{**ex.to_bronze_record(), "snapshot_date": snapshot_date} for ex in exchanges]
    written = lake.insert_rows("bronze", "exchanges", records)
    log.info("exchanges.write_done", snapshot_date=snapshot_date, rows=written)
    return written
