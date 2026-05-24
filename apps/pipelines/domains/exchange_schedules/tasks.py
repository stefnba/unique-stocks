"""Prefect tasks for exchange schedules and holidays ingestion."""

from datetime import UTC, date, datetime

import httpx
import structlog
from prefect import task

from config.blocks import BlockRegistry
from core.ingestion import already_ingested, save_landing, write_bronze
from domains.exchange_schedules.datasets import (
    EXCHANGE_HOLIDAYS_DATASET,
    EXCHANGE_SCHEDULES_DATASET,
    EXCHANGE_SCHEDULES_LANDING,
)
from domains.exchange_schedules.parsers import parse_exchange_holiday_snapshots, parse_exchange_schedule_snapshot
from providers.eodhd.models import ExchangeSchedule

log = structlog.get_logger(__name__)


@task(name="fetch-schedule-exchange-codes")
async def fetch_schedule_exchange_codes() -> list[str]:
    """Load exchange codes from the v2 schedule API list endpoint.

    The v2 endpoint uses MIC-style codes (e.g. ``XETR``) that differ from the
    catalog codes in ``/exchanges-list`` (e.g. ``XETRA``). Using catalog codes
    mostly results in 404s on the detail endpoint.
    """
    from providers.eodhd.client import EODHDClient

    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        codes = await client.get_exchange_details_codes()

    log.info("schedules.codes_loaded", source="v2_list", count=len(codes))
    return codes


@task(name="fetch-exchange-details", retries=3, retry_delay_seconds=10)
async def fetch_exchange_details(exchange_code: str) -> ExchangeSchedule | None:
    """Fetch v2 trading hours and holidays for one exchange.

    Returns None when the exchange is not supported by the v2 endpoint.
    """
    from providers.eodhd.client import EODHDClient

    log.info("schedules.fetch_start", exchange=exchange_code)
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        try:
            details = await client.get_exchange_details(exchange_code)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                log.warning(
                    "schedules.exchange_unsupported",
                    exchange=exchange_code,
                    status=exc.response.status_code,
                )
                return None
            raise

    log.info("schedules.fetch_done", exchange=exchange_code)
    return details


@task(name="write-schedule-landing")
async def write_schedule_to_landing_zone(
    details: ExchangeSchedule,
    exchange_code: str,
    ingested_at: datetime | None = None,
) -> str:
    """Write raw exchange-details JSON to the S3 landing zone."""
    from core.clients.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = save_landing(
        s3,
        EXCHANGE_SCHEDULES_LANDING,
        EXCHANGE_SCHEDULES_DATASET.provider,
        details,
        ingested_at=stamp,
        exchange=exchange_code,
    )
    return ref.uri


@task(name="write-bronze-exchange-schedule")
def write_bronze_exchange_schedule(
    details: ExchangeSchedule,
    snapshot_date: date,
    source_uri: str | None = None,
) -> int:
    """Write one exchange schedule row to bronze.exchange_schedules."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    if already_ingested(
        lake,
        EXCHANGE_SCHEDULES_DATASET,
        snapshot_date=snapshot_date,
        exchange_code=details.exchange_code,
    ):
        log.info(
            "schedules.write_skipped",
            reason="already_ingested",
            exchange=details.exchange_code,
            snapshot_date=snapshot_date,
        )
        return 0

    written = write_bronze(
        lake,
        EXCHANGE_SCHEDULES_DATASET,
        [parse_exchange_schedule_snapshot(details, snapshot_date)],
        source_uri=source_uri,
    )
    log.info(
        "schedules.write_done",
        exchange=details.exchange_code,
        snapshot_date=snapshot_date,
        rows=written,
    )
    return written


@task(name="write-bronze-exchange-holidays")
def write_bronze_exchange_holidays(
    details: ExchangeSchedule,
    snapshot_date: date,
    source_uri: str | None = None,
) -> int:
    """Write holiday rows for one exchange to bronze.exchange_holidays."""
    from core.clients.lake import get_lake_client

    exchange_code = details.exchange_code
    holidays = parse_exchange_holiday_snapshots(details, snapshot_date)
    if not holidays:
        log.info(
            "schedules.holidays_write_skipped",
            reason="no_holidays",
            exchange=exchange_code,
            snapshot_date=snapshot_date,
        )
        return 0

    lake = get_lake_client()
    if already_ingested(
        lake,
        EXCHANGE_HOLIDAYS_DATASET,
        snapshot_date=snapshot_date,
        exchange_code=exchange_code,
    ):
        log.info(
            "schedules.holidays_write_skipped",
            reason="already_ingested",
            exchange=exchange_code,
            snapshot_date=snapshot_date,
        )
        return 0

    written = write_bronze(lake, EXCHANGE_HOLIDAYS_DATASET, holidays, source_uri=source_uri)
    log.info(
        "schedules.holidays_write_done",
        exchange=exchange_code,
        snapshot_date=snapshot_date,
        rows=written,
    )
    return written


@task(name="schedule-already-ingested")
def schedule_already_ingested(exchange_code: str, snapshot_date: date) -> bool:
    """Return True when schedule data for this exchange and snapshot already exists."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    return already_ingested(
        lake,
        EXCHANGE_SCHEDULES_DATASET,
        snapshot_date=snapshot_date,
        exchange_code=exchange_code,
    )
