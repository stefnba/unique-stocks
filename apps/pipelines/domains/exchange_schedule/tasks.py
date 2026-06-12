"""Prefect tasks for exchange schedule and holiday ingestion."""

from datetime import UTC, date, datetime

import httpx
import structlog
from prefect import task
from prefect.client.schemas.objects import State, TaskRun
from prefect.tasks import exponential_backoff
from pydantic import ValidationError

from config.blocks import BlockRegistry
from core.clients.http.base import ProviderRateLimitError
from core.ingestion import BronzeWrite, LandingWrite
from domains.exchange_schedule.datasets import (
    EXCHANGE_HOLIDAY_DATASET,
    EXCHANGE_SCHEDULE_DATASET,
)
from domains.exchange_schedule.parsers import parse_exchange_holiday_snapshots, parse_exchange_schedule_snapshot
from providers.eodhd.models import ExchangeSchedule

log = structlog.get_logger(__name__)


def _is_retryable(task: object, task_run: TaskRun, state: State) -> bool:
    """Retry transient errors only; never retry schema drift or provider quota exhaustion."""
    exc = state.result(raise_on_failure=False)
    return not isinstance(exc, ValidationError | ProviderRateLimitError)


@task(name="fetch-provider-schedule-exchange-codes")
async def fetch_provider_schedule_exchange_codes() -> list[str]:
    """Load provider schedule exchange codes from the v2 schedule API list endpoint.

    These are endpoint-specific provider codes. Some look like MICs (e.g.
    ``XETR``), while others are provider aggregate codes (e.g. ``US``). Using
    catalog codes can result in 404s when the v2 endpoint expects a different
    code for that market.
    """
    from providers.eodhd.client import EODHDClient

    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        codes = await client.get_exchange_details_codes()

    log.info("schedule.provider_schedule_exchange_codes_loaded", source="v2_list", count=len(codes))
    return codes


@task(name="resolve-operational-schedule-exchange-codes")
def resolve_operational_schedule_exchange_codes(available_schedule_codes: list[str]) -> list[str]:
    """Intersect live provider schedule support with the dbt operational universe."""
    from domains.exchange.provider_universe import load_provider_schedule_exchange_codes

    codes = load_provider_schedule_exchange_codes(
        "eodhd",
        available_schedule_codes=available_schedule_codes,
        purpose="eod_price",
    )
    log.info(
        "schedule.operational_schedule_exchange_codes_resolved",
        available_count=len(available_schedule_codes),
        selected_count=len(codes),
    )
    return codes


@task(
    name="fetch-exchange-details",
    task_run_name="fetch-exchange-details-{provider_schedule_exchange_code}",
    retries=3,
    retry_delay_seconds=exponential_backoff(10),
    retry_condition_fn=_is_retryable,
)
async def fetch_exchange_details(provider_schedule_exchange_code: str) -> ExchangeSchedule | None:
    """Fetch v2 trading hours and holiday for one exchange.

    Returns None when the exchange is not supported by the v2 endpoint.
    """
    from providers.eodhd.client import EODHDClient

    log.info("schedule.fetch_start", provider_schedule_exchange_code=provider_schedule_exchange_code)
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        try:
            details = await client.get_exchange_details(provider_schedule_exchange_code)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                log.warning(
                    "schedule.exchange_unsupported",
                    provider_schedule_exchange_code=provider_schedule_exchange_code,
                    status=exc.response.status_code,
                )
                return None
            raise

    log.info("schedule.fetch_done", provider_schedule_exchange_code=provider_schedule_exchange_code)
    return details


@task(
    name="write-schedule-landing",
    task_run_name="write-schedule-landing-{provider_schedule_exchange_code}",
)
async def write_schedule_to_landing_zone(
    details: ExchangeSchedule,
    provider_schedule_exchange_code: str,
    snapshot_date: date,
    ingested_at: datetime | None = None,
) -> LandingWrite:
    """Write raw exchange-details JSON to the S3 landing zone."""
    from core.clients.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = EXCHANGE_SCHEDULE_DATASET.landings.details.save(
        s3,
        provider=EXCHANGE_SCHEDULE_DATASET.provider,
        data=details,
        partitions={
            "provider_schedule_exchange_code": provider_schedule_exchange_code,
            "snapshot_date": snapshot_date,
        },
        ingested_at=stamp,
    )
    return EXCHANGE_SCHEDULE_DATASET.landings.details.landing_write(
        ref,
        partitions={
            "provider_schedule_exchange_code": provider_schedule_exchange_code,
            "snapshot_date": snapshot_date,
        },
        rows_raw=1,
    )


@task(
    name="write-bronze-exchange-schedule",
    task_run_name="write-bronze-exchange-schedule-{details.provider_schedule_exchange_code}",
)
def write_bronze_exchange_schedule(
    details: ExchangeSchedule,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write one exchange schedule row to bronze.exchange_schedule."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    if EXCHANGE_SCHEDULE_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_schedule_exchange_code=details.provider_schedule_exchange_code,
    ):
        log.info(
            "schedule.write_skipped",
            reason="already_ingested",
            provider_schedule_exchange_code=details.provider_schedule_exchange_code,
            snapshot_date=snapshot_date,
        )
        return BronzeWrite(rows_written=0, reason="already_ingested")

    written = EXCHANGE_SCHEDULE_DATASET.write_bronze(
        lake,
        [parse_exchange_schedule_snapshot(details, snapshot_date)],
        source_uri=source_uri,
    )
    log.info(
        "schedule.write_done",
        provider_schedule_exchange_code=details.provider_schedule_exchange_code,
        snapshot_date=snapshot_date,
        rows=written,
    )
    return BronzeWrite(rows_written=written)


@task(
    name="write-bronze-exchange-holiday",
    task_run_name="write-bronze-exchange-holiday-{details.provider_schedule_exchange_code}",
)
def write_bronze_exchange_holiday(
    details: ExchangeSchedule,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write holiday rows for one exchange to bronze.exchange_holiday."""
    from core.clients.lake import get_lake_client

    provider_schedule_exchange_code = details.provider_schedule_exchange_code
    holiday = parse_exchange_holiday_snapshots(details, snapshot_date)
    if not holiday:
        log.info(
            "schedule.holiday_write_skipped",
            reason="no_holiday",
            provider_schedule_exchange_code=provider_schedule_exchange_code,
            snapshot_date=snapshot_date,
        )
        return BronzeWrite(rows_written=0, reason="no_holiday")

    lake = get_lake_client()
    if EXCHANGE_HOLIDAY_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_schedule_exchange_code=provider_schedule_exchange_code,
    ):
        log.info(
            "schedule.holiday_write_skipped",
            reason="already_ingested",
            provider_schedule_exchange_code=provider_schedule_exchange_code,
            snapshot_date=snapshot_date,
        )
        return BronzeWrite(rows_written=0, reason="already_ingested")

    written = EXCHANGE_HOLIDAY_DATASET.write_bronze(lake, holiday, source_uri=source_uri)
    log.info(
        "schedule.holiday_write_done",
        provider_schedule_exchange_code=provider_schedule_exchange_code,
        snapshot_date=snapshot_date,
        rows=written,
    )
    return BronzeWrite(rows_written=written)


@task(
    name="schedule-already-ingested",
    task_run_name="schedule-already-ingested-{provider_schedule_exchange_code}",
)
def schedule_already_ingested(provider_schedule_exchange_code: str, snapshot_date: date) -> bool:
    """Return True when schedule data for this exchange and snapshot already exists."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    return EXCHANGE_SCHEDULE_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_schedule_exchange_code=provider_schedule_exchange_code,
    )
