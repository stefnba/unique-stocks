"""Prefect tasks for instrument ingestion."""

from datetime import UTC, date, datetime

import structlog
from prefect import task

from config.blocks import BlockRegistry
from core.ingestion import already_ingested, save_landing, write_bronze
from domains.instruments.datasets import INSTRUMENTS_DATASET, INSTRUMENTS_LANDING
from domains.instruments.parsers import parse_instrument_snapshots
from providers.eodhd.models import Instrument

log = structlog.get_logger(__name__)


@task(name="fetch-instrument-exchange-codes")
async def fetch_instrument_exchange_codes() -> list[str]:
    """All exchange codes from the EODHD catalog, including virtual asset classes.

    Pass ``exchange_codes`` to the flow to restrict ingestion to a subset
    (e.g. equities only, or crypto only). Use separate flow deployments to
    run different asset classes on different schedules.
    """
    from providers.eodhd.client import EODHDClient

    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        exchanges = await client.get_exchanges()

    codes = [ex.exchange_code for ex in exchanges]
    log.info("instruments.codes_loaded", count=len(codes))
    return codes


@task(name="fetch-instruments", retries=3, retry_delay_seconds=10)
async def fetch_instruments(exchange_code: str) -> list[Instrument]:
    """Fetch all active instruments for one exchange.

    For US equities pass exchange_code="US" — it covers NYSE, NASDAQ,
    NYSE ARCA, and OTC in a single call. The sub-exchange per instrument is
    available in the ``exchange`` field of the response.
    """
    from providers.eodhd.client import EODHDClient

    log.info("instruments.fetch_start", exchange=exchange_code)
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        instruments = await client.get_instruments(exchange_code)

    log.info("instruments.fetch_done", exchange=exchange_code, count=len(instruments))
    return instruments


@task(name="write-instruments-landing")
async def write_instruments_to_landing_zone(
    instruments: list[Instrument],
    exchange_code: str,
    ingested_at: datetime | None = None,
) -> str:
    """Write raw instrument list for one exchange to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = save_landing(
        s3,
        INSTRUMENTS_LANDING,
        INSTRUMENTS_DATASET.provider,
        instruments,
        ingested_at=stamp,
        exchange=exchange_code,
    )
    return ref.uri


@task(name="write-bronze-instruments")
def write_bronze_instruments(
    instruments: list[Instrument],
    exchange_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> int:
    """Write instrument rows for one exchange to bronze.instruments.

    ``exchange_code`` is the API call code (e.g. "US", "FOREX", "CC"). The
    sub-exchange per instrument (e.g. "NYSE", "NASDAQ") comes from the model's
    ``exchange`` field. Idempotency is checked at the (exchange_code, snapshot_date) level.
    """
    from core.clients.lake import get_lake_client

    if not instruments:
        log.info("instruments.write_skipped", reason="no_data", exchange=exchange_code)
        return 0

    lake = get_lake_client()
    if already_ingested(lake, INSTRUMENTS_DATASET, snapshot_date=snapshot_date, exchange_code=exchange_code):
        log.info(
            "instruments.write_skipped",
            reason="already_ingested",
            exchange=exchange_code,
            snapshot_date=snapshot_date,
        )
        return 0

    sources, rejected = parse_instrument_snapshots(instruments, exchange_code, snapshot_date)
    if rejected:
        log.warning(
            "instruments.parse_rejections",
            exchange=exchange_code,
            snapshot_date=snapshot_date,
            count=len(rejected),
        )
    written = write_bronze(lake, INSTRUMENTS_DATASET, sources, source_uri=source_uri)
    log.info(
        "instruments.write_done",
        exchange=exchange_code,
        snapshot_date=snapshot_date,
        rows=written,
    )
    return written


@task(name="instruments-already-ingested")
def instruments_already_ingested(exchange_code: str, snapshot_date: date) -> bool:
    """Return True when instrument data for this exchange and snapshot already exists."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    return already_ingested(lake, INSTRUMENTS_DATASET, snapshot_date=snapshot_date, exchange_code=exchange_code)
