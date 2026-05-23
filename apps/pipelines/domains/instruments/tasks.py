"""Prefect tasks for instrument ingestion."""

from datetime import UTC, date, datetime

import structlog
from prefect import task

from config.blocks import BlockRegistry
from core.clients.storage.s3 import S3Key
from providers.eodhd.models import Instrument
from providers.registry import Provider

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
    key = S3Key.partitioned(
        S3Key.Provider.EODHD,
        S3Key.Domain.INSTRUMENTS,
        exchange=exchange_code,
        ingested_at=stamp,
    ).jsonl()
    ref = s3.save(key, instruments)
    return ref.uri


@task(name="write-bronze-instruments")
def write_bronze_instruments(
    instruments: list[Instrument],
    exchange_code: str,
    snapshot_date: date,
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
    qualified = lake.qualified_name("bronze", "instruments")
    already_ingested = lake.query_one(
        f"""
        SELECT COUNT(*) AS cnt FROM {qualified}
        WHERE snapshot_date = ? AND exchange_code = ? AND provider = ?
        """,
        [snapshot_date.isoformat(), exchange_code, Provider.EODHD],
    )
    if already_ingested and already_ingested["cnt"] > 0:
        log.info(
            "instruments.write_skipped",
            reason="already_ingested",
            exchange=exchange_code,
            snapshot_date=snapshot_date,
        )
        return 0

    records = [
        {**i.to_bronze_record(), "exchange_code": exchange_code, "snapshot_date": snapshot_date}
        for i in instruments
    ]
    written = lake.insert_rows("bronze", "instruments", records)
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
    qualified = lake.qualified_name("bronze", "instruments")
    row = lake.query_one(
        f"""
        SELECT COUNT(*) AS cnt FROM {qualified}
        WHERE snapshot_date = ? AND exchange_code = ? AND provider = ?
        """,
        [snapshot_date.isoformat(), exchange_code, Provider.EODHD],
    )
    return bool(row and row["cnt"] > 0)
