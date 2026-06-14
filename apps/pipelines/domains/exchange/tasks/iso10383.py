"""Prefect tasks for ISO 10383 MIC registry ingestion."""

from datetime import date

import structlog
from prefect import task

from control_plane.prefect_blocks import BlockRegistry
from core.ingestion import BronzeWrite, LandingWrite
from domains.exchange.datasets import EXCHANGE_MIC_REGISTRY_DATASET
from domains.exchange.parsers import (
    load_iso10383_mic_csv,
    parse_exchange_mic_registry_snapshots,
    parse_iso10383_mic_raw_rows,
)
from providers.iso10383.client import ISO10383Client
from providers.iso10383.models import ISO10383MICRaw

log = structlog.get_logger(__name__)


@task(name="fetch-iso10383-mic-csv", retries=3, retry_delay_seconds=10)
async def fetch_iso10383_mic_csv() -> str:
    """Fetch the ISO 10383 MIC registry CSV release."""
    async with ISO10383Client() as client:
        csv_text = await client.get_mic_registry_csv()
    log.info("exchange_mic_registry.fetch_done", bytes=len(csv_text))
    return csv_text


@task(name="load-iso10383-mic-raw-rows")
def load_iso10383_mic_raw_rows(csv_text: str) -> list[dict[str, str]]:
    """Load raw ISO 10383 CSV text into dictionaries for landing and validation."""
    rows = load_iso10383_mic_csv(csv_text)
    log.info("exchange_mic_registry.csv_loaded", rows=len(rows))
    return rows


@task(name="write-exchange-mic-registry-landing")
async def write_mic_registry_to_landing_zone(
    raw_rows: list[dict[str, str]],
    snapshot_date: date,
) -> LandingWrite:
    """Write the ISO MIC registry CSV rows to the S3 landing zone."""
    from core.storage.s3 import S3StorageClient

    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = EXCHANGE_MIC_REGISTRY_DATASET.landings.mic_registry.save(
        s3,
        provider=EXCHANGE_MIC_REGISTRY_DATASET.provider,
        data=raw_rows,
        snapshot_date=snapshot_date,
    )
    return EXCHANGE_MIC_REGISTRY_DATASET.landings.mic_registry.landing_write(
        ref,
        snapshot_date=snapshot_date,
        rows_raw=len(raw_rows),
    )


@task(name="parse-iso10383-mic-registry")
def parse_iso10383_mic_registry(
    raw_rows: list[dict[str, str]],
) -> tuple[list[ISO10383MICRaw], list[dict[str, str]]]:
    """Validate ISO MIC registry raw rows."""
    valid, rejected = parse_iso10383_mic_raw_rows(raw_rows)
    if rejected:
        log.warning("exchange_mic_registry.parse_rejections", count=len(rejected))
    log.info("exchange_mic_registry.parsed", valid=len(valid), rejected=len(rejected))
    return valid, rejected


@task(name="write-bronze-exchange-mic-registry")
def write_bronze_exchange_mic_registry(
    mic_rows: list[ISO10383MICRaw],
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write validated ISO MIC registry rows to bronze.exchange_mic_registry."""
    from core.lake import get_lake_client

    if not mic_rows:
        log.info("exchange_mic_registry.write_skipped", reason="no_data", snapshot_date=snapshot_date)
        return BronzeWrite(rows_written=0, reason="no_data")

    lake = get_lake_client()
    if EXCHANGE_MIC_REGISTRY_DATASET.already_ingested(lake, snapshot_date=snapshot_date):
        log.info(
            "exchange_mic_registry.write_skipped",
            reason="already_ingested",
            snapshot_date=snapshot_date,
        )
        return BronzeWrite(rows_written=0, reason="already_ingested")

    sources = parse_exchange_mic_registry_snapshots(mic_rows, snapshot_date)
    written = EXCHANGE_MIC_REGISTRY_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("exchange_mic_registry.write_done", snapshot_date=snapshot_date, rows=written)
    return BronzeWrite(rows_written=written)
