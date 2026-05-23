"""Prefect tasks for exchange schedules and holidays ingestion."""

from datetime import UTC, date, datetime

import httpx
import structlog
from prefect import task

from config.blocks import BlockRegistry
from core.clients.storage.s3 import S3Key
from domains.exchange_schedules.parsers import parse_holiday_records, parse_schedule_record
from providers.eodhd.models import ExchangeSchedule
from providers.registry import Provider

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
    key = S3Key.partitioned(
        S3Key.Provider.EODHD,
        S3Key.Domain.EXCHANGE_SCHEDULES,
        exchange=exchange_code,
        ingested_at=stamp,
    ).json()
    ref = s3.save(key, details.model_dump(mode="json", by_alias=False))
    return ref.uri


@task(name="write-bronze-exchange-schedule")
def write_bronze_exchange_schedule(details: ExchangeSchedule, snapshot_date: date) -> int:
    """Write one exchange schedule row to bronze.exchange_schedules."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    qualified = lake.qualified_name("bronze", "exchange_schedules")
    already_ingested = lake.query_one(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {qualified}
        WHERE snapshot_date = ? AND exchange_code = ? AND provider = ?
        """,
        [snapshot_date.isoformat(), details.exchange_code, Provider.EODHD],
    )
    if already_ingested and already_ingested["cnt"] > 0:
        log.info(
            "schedules.write_skipped",
            reason="already_ingested",
            exchange=details.exchange_code,
            snapshot_date=snapshot_date,
        )
        return 0

    written = lake.insert_rows(
        "bronze",
        "exchange_schedules",
        [parse_schedule_record(details, snapshot_date)],
    )
    log.info(
        "schedules.write_done",
        exchange=details.exchange_code,
        snapshot_date=snapshot_date,
        rows=written,
    )
    return written


@task(name="write-bronze-exchange-holidays")
def write_bronze_exchange_holidays(details: ExchangeSchedule, snapshot_date: date) -> int:
    """Write holiday rows for one exchange to bronze.exchange_holidays."""
    from core.clients.lake import get_lake_client

    exchange_code = details.exchange_code
    holidays = parse_holiday_records(details, snapshot_date)
    if not holidays:
        log.info(
            "schedules.holidays_write_skipped",
            reason="no_holidays",
            exchange=exchange_code,
            snapshot_date=snapshot_date,
        )
        return 0

    lake = get_lake_client()
    qualified = lake.qualified_name("bronze", "exchange_holidays")
    already_ingested = lake.query_one(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {qualified}
        WHERE snapshot_date = ? AND exchange_code = ? AND provider = ?
        """,
        [snapshot_date.isoformat(), exchange_code, Provider.EODHD],
    )
    if already_ingested and already_ingested["cnt"] > 0:
        log.info(
            "schedules.holidays_write_skipped",
            reason="already_ingested",
            exchange=exchange_code,
            snapshot_date=snapshot_date,
        )
        return 0

    written = lake.insert_rows("bronze", "exchange_holidays", holidays)
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
    qualified = lake.qualified_name("bronze", "exchange_schedules")
    row = lake.query_one(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {qualified}
        WHERE snapshot_date = ? AND exchange_code = ? AND provider = ?
        """,
        [snapshot_date.isoformat(), exchange_code, Provider.EODHD],
    )
    return bool(row and row["cnt"] > 0)
