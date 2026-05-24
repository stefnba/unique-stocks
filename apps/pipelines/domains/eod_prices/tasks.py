"""Prefect tasks for EOD price ingestion.

Tasks are atomic, retryable units. Each task does exactly one thing:
fetch, validate, or write. No business logic.
"""

from datetime import UTC, date, datetime

import structlog
from prefect import task
from prefect.client.schemas.objects import State, TaskRun
from prefect.tasks import exponential_backoff
from pydantic import ValidationError

from config.blocks import BlockRegistry
from core.clients.storage.s3 import S3Key
from providers.eodhd.client import EODHDClient
from providers.eodhd.models import EODBulkPriceRaw
from providers.registry import Provider

from .models import EODBar
from .parsers import bars_to_bronze_records, parse_eod_bars

log = structlog.get_logger(__name__)


@task(name="fetch-eod-exchange-codes")
async def fetch_eod_exchange_codes() -> list[str]:
    """Exchange codes eligible for bulk EOD ingestion.

    Loads distinct codes already present in bronze.exchanges. Falls back to
    ["US"] if the table is empty (e.g. on a fresh environment before the
    exchanges flow has run).
    """
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    if not lake.table_exists("bronze", "exchanges"):
        log.warning("prices.exchange_codes_fallback", reason="bronze.exchanges missing")
        return ["US"]

    qualified = lake.qualified_name("bronze", "exchanges")
    rows = lake.query(f"SELECT DISTINCT exchange_code FROM {qualified} ORDER BY exchange_code")
    codes = [r["exchange_code"] for r in rows] if rows else ["US"]
    log.info("prices.exchange_codes_loaded", count=len(codes))
    return codes


def _is_retryable(task: object, task_run: TaskRun, state: State) -> bool:
    """Retry transient errors only — never retry schema validation failures."""
    exc = state.result(raise_on_failure=False)
    return not isinstance(exc, ValidationError)


@task(
    name="fetch-eod-prices-bulk",
    retries=3,
    retry_delay_seconds=exponential_backoff(10),
    retry_condition_fn=_is_retryable,
    log_prints=True,
)
async def fetch_eod_prices_bulk(exchange: str, bar_date: date) -> list[EODBulkPriceRaw]:
    """Fetch and schema-validate raw EOD price rows for an entire exchange.

    Uses the EODHD bulk endpoint (one API call per exchange per date).
    Raises ValidationError if EODHD's response shape doesn't match EODBulkPriceRaw.
    """
    log.info("prices.fetch_start", exchange=exchange, bar_date=bar_date)
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        rows = await client.get_eod_prices_bulk(exchange=exchange, bar_date=bar_date)
    log.info("prices.fetch_done", exchange=exchange, bar_date=bar_date, rows=len(rows))
    return rows


@task(name="write-eod-prices-landing")
async def write_eod_prices_to_landing(
    raw_rows: list[EODBulkPriceRaw],
    exchange: str,
    bar_date: date,
    ingested_at: datetime | None = None,
) -> str:
    """Write raw bulk price rows for one exchange to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    key = S3Key.partitioned(
        S3Key.Provider.EODHD,
        S3Key.Domain.EOD_PRICES,
        exchange=exchange,
        bar_date=bar_date,
        ingested_at=stamp,
    ).jsonl()
    ref = s3.save(key, raw_rows)
    log.info("prices.landing_written", exchange=exchange, bar_date=bar_date, uri=ref.uri)
    return ref.uri


@task(name="parse-eod-prices")
def parse_eod_prices(
    raw_rows: list[EODBulkPriceRaw],
    bar_date: date,
    exchange: str,
) -> list[EODBar]:
    """Apply business validation and convert raw rows to EODBar domain models.

    Logs and drops invalid rows — a few bad tickers should not abort an
    entire exchange's worth of data.
    """
    valid, rejected = parse_eod_bars(raw_rows, expected_date=bar_date, exchange=exchange)
    if rejected:
        log.warning(
            "prices.parse_rejections",
            count=len(rejected),
            bar_date=bar_date,
            exchange=exchange,
        )
    log.info("prices.parsed", valid=len(valid), rejected=len(rejected), bar_date=bar_date)
    return valid


@task(name="write-bronze-eod-prices")
def write_bronze_eod_prices(bars: list[EODBar], exchange: str, bar_date: date) -> int:
    """Write validated bars to bronze.eod_prices.

    Idempotency is checked at the (exchange_code, bar_date, provider) level —
    re-running the flow for the same exchange + date is safe.
    """
    from core.clients.lake import get_lake_client

    if not bars:
        log.info("prices.write_skipped", reason="no_bars", exchange=exchange, bar_date=bar_date)
        return 0

    lake = get_lake_client()
    qualified = lake.qualified_name("bronze", "eod_prices")
    already_ingested = lake.query_one(
        f"""
        SELECT COUNT(*) AS cnt FROM {qualified}
        WHERE exchange_code = ? AND bar_date = ? AND provider = ?
        """,
        [exchange, bar_date.isoformat(), Provider.EODHD],
    )
    if already_ingested and already_ingested["cnt"] > 0:
        log.info(
            "prices.write_skipped",
            reason="already_ingested",
            exchange=exchange,
            bar_date=bar_date,
        )
        return 0

    records = bars_to_bronze_records(bars, exchange_code=exchange)
    written = lake.insert_rows("bronze", "eod_prices", records)
    log.info("prices.write_done", exchange=exchange, bar_date=bar_date, rows=written)
    return written
