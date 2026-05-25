"""Prefect tasks for instrument ingestion."""

from datetime import UTC, date, datetime

import structlog
from prefect import task

from config.blocks import BlockRegistry
from domains.instrument.datasets import INSTRUMENT_DATASET
from domains.instrument.parsers import parse_instrument_snapshots
from providers.eodhd.models import Instrument

log = structlog.get_logger(__name__)


@task(name="fetch-instrument-provider-exchange-codes")
async def fetch_instrument_provider_exchange_codes() -> list[str]:
    """All provider exchange codes from the EODHD catalog, including virtual asset classes.

    Pass ``provider_exchange_codes`` to the flow to restrict ingestion to a
    subset (e.g. equities only, or crypto only). Use separate flow deployments
    to run different asset classes on different schedule.
    """
    from providers.eodhd.client import EODHDClient

    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        exchange = await client.get_exchange()

    codes = [ex.provider_exchange_code for ex in exchange]
    log.info("instrument.provider_exchange_codes_loaded", count=len(codes))
    return codes


@task(name="fetch-instrument", retries=3, retry_delay_seconds=10)
async def fetch_instrument(provider_exchange_code: str) -> list[Instrument]:
    """Fetch all active instrument for one exchange.

    For US equities pass provider_exchange_code="US" — it covers NYSE, NASDAQ,
    NYSE ARCA, and OTC in a single call. The per-row provider listing exchange
    code is available in ``provider_listing_exchange_code``.
    """
    from providers.eodhd.client import EODHDClient

    log.info("instrument.fetch_start", provider_exchange_code=provider_exchange_code)
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        instrument = await client.get_instrument(provider_exchange_code)

    log.info("instrument.fetch_done", provider_exchange_code=provider_exchange_code, count=len(instrument))
    return instrument


@task(name="write-instrument-landing")
async def write_instrument_to_landing_zone(
    instrument: list[Instrument],
    provider_exchange_code: str,
    snapshot_date: date,
    ingested_at: datetime | None = None,
) -> str:
    """Write raw instrument list for one exchange to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = INSTRUMENT_DATASET.landings.catalog.save(
        s3,
        provider=INSTRUMENT_DATASET.provider,
        data=instrument,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "snapshot_date": snapshot_date,
        },
        ingested_at=stamp,
    )
    return ref.uri


@task(name="write-bronze-instrument")
def write_bronze_instrument(
    instrument: list[Instrument],
    provider_exchange_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> int:
    """Write instrument rows for one exchange to bronze.instrument.

    ``provider_exchange_code`` is the EODHD API call code (e.g. "US", "FOREX",
    "CC"). The per-row listing exchange code (e.g. "NYSE", "NASDAQ") comes
    from the model's ``provider_listing_exchange_code`` field. Idempotency is
    checked at the (provider_exchange_code, snapshot_date) level.
    """
    from core.clients.lake import get_lake_client

    if not instrument:
        log.info("instrument.write_skipped", reason="no_data", provider_exchange_code=provider_exchange_code)
        return 0

    lake = get_lake_client()
    if INSTRUMENT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
    ):
        log.info(
            "instrument.write_skipped",
            reason="already_ingested",
            provider_exchange_code=provider_exchange_code,
            snapshot_date=snapshot_date,
        )
        return 0

    sources, rejected = parse_instrument_snapshots(instrument, provider_exchange_code, snapshot_date)
    if rejected:
        log.warning(
            "instrument.parse_rejections",
            provider_exchange_code=provider_exchange_code,
            snapshot_date=snapshot_date,
            count=len(rejected),
        )
    written = INSTRUMENT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info(
        "instrument.write_done",
        provider_exchange_code=provider_exchange_code,
        snapshot_date=snapshot_date,
        rows=written,
    )
    return written


@task(name="instrument-already-ingested")
def instrument_already_ingested(provider_exchange_code: str, snapshot_date: date) -> bool:
    """Return True when instrument data for this exchange and snapshot already exists."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    return INSTRUMENT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
    )
