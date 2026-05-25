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
from core.ingestion import BronzeParseResult
from providers.eodhd.client import EODHDClient
from providers.eodhd.models import EODBulkPriceRaw, EODPriceBarRaw

from .datasets import EOD_PRICE_DATASET
from .models import EODBar
from .parsers import parse_eod_bars
from .symbols import qualified_ticker, ticker_without_exchange

log = structlog.get_logger(__name__)


@task(name="fetch-eod-exchange-codes")
async def fetch_eod_exchange_codes() -> list[str]:
    """Exchange codes eligible for bulk EOD ingestion.

    Loads distinct codes already present in bronze.exchange. Falls back to
    ["US"] if the table is empty (e.g. on a fresh environment before the
    exchange flow has run).
    """
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    if not lake.table_exists("bronze", "exchange"):
        log.warning("price.exchange_codes_fallback", reason="bronze.exchange missing")
        return ["US"]

    qualified = lake.qualified_name("bronze", "exchange")
    rows = lake.query(f"SELECT DISTINCT exchange_code FROM {qualified} ORDER BY exchange_code")
    codes = [r["exchange_code"] for r in rows] if rows else ["US"]
    log.info("price.exchange_codes_loaded", count=len(codes))
    return codes


def _is_retryable(task: object, task_run: TaskRun, state: State) -> bool:
    """Retry transient errors only — never retry schema validation failures."""
    exc = state.result(raise_on_failure=False)
    return not isinstance(exc, ValidationError)


@task(
    name="fetch-eod-price-bulk",
    retries=3,
    retry_delay_seconds=exponential_backoff(10),
    retry_condition_fn=_is_retryable,
    log_prints=True,
)
async def fetch_eod_price_bulk(exchange: str, bar_date: date) -> list[EODBulkPriceRaw]:
    """Fetch and schema-validate raw EOD price rows for an entire exchange.

    Uses the EODHD bulk endpoint (one API call per exchange per date).
    Raises ValidationError if EODHD's response shape doesn't match EODBulkPriceRaw.
    """
    log.info("price.fetch_start", exchange=exchange, bar_date=bar_date)
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        rows = await client.get_eod_price_bulk(exchange=exchange, bar_date=bar_date)
    log.info("price.fetch_done", exchange=exchange, bar_date=bar_date, rows=len(rows))
    return rows


@task(name="write-eod-price-landing")
async def write_eod_price_to_landing(
    raw_rows: list[EODBulkPriceRaw],
    exchange: str,
    bar_date: date,
    ingested_at: datetime | None = None,
) -> str:
    """Write raw bulk price rows for one exchange to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = EOD_PRICE_DATASET.landings.daily.save(
        s3,
        provider=EOD_PRICE_DATASET.provider,
        data=raw_rows,
        partitions={
            "exchange": exchange,
            "bar_date": bar_date,
        },
        ingested_at=stamp,
    )
    log.info("price.landing_written", exchange=exchange, bar_date=bar_date, uri=ref.uri)
    return ref.uri


@task(name="write-ticker-eod-history-landing")
async def write_ticker_eod_history_to_landing(
    raw_bars: list[EODPriceBarRaw],
    symbol: str,
    exchange_code: str,
    from_date: date,
    to_date: date,
    ingested_at: datetime | None = None,
) -> str:
    """Write raw per-ticker historical bars to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = EOD_PRICE_DATASET.landings.backfill.save(
        s3,
        provider=EOD_PRICE_DATASET.provider,
        data=raw_bars,
        partitions={
            "exchange": exchange_code,
            "ticker": symbol,
            "from_date": from_date,
            "to_date": to_date,
        },
        ingested_at=stamp,
    )
    log.info("backfill.landing_written", symbol=symbol, uri=ref.uri)
    return ref.uri


@task(name="parse-eod-price")
def parse_eod_price(
    raw_rows: list[EODBulkPriceRaw],
    bar_date: date,
    exchange: str,
) -> list[BronzeParseResult[EODBar]]:
    """Apply business validation and convert raw rows to EODBar domain models.

    Logs and drops invalid rows — a few bad tickers should not abort an
    entire exchange's worth of data.
    """
    valid, rejected = parse_eod_bars(raw_rows, expected_date=bar_date, exchange=exchange)
    if rejected:
        log.warning(
            "price.parse_rejections",
            count=len(rejected),
            bar_date=bar_date,
            exchange=exchange,
        )
    log.info("price.parsed", valid=len(valid), rejected=len(rejected), bar_date=bar_date)
    return valid


@task(name="write-bronze-eod-price")
def write_bronze_eod_price(
    sources: list[BronzeParseResult[EODBar]],
    exchange: str,
    bar_date: date,
    source_uri: str | None = None,
) -> int:
    """Write validated bars to bronze.eod_price.

    Idempotency is checked at the (exchange_code, bar_date) level — the
    ``data_provider`` column is enforced in ``already_ingested``. Re-running the
    flow for the same exchange + date is safe.
    """
    from core.clients.lake import get_lake_client

    if not sources:
        log.info("price.write_skipped", reason="no_bars", exchange=exchange, bar_date=bar_date)
        return 0

    lake = get_lake_client()
    if EOD_PRICE_DATASET.already_ingested(lake, exchange_code=exchange, bar_date=bar_date):
        log.info(
            "price.write_skipped",
            reason="already_ingested",
            exchange=exchange,
            bar_date=bar_date,
        )
        return 0

    written = EOD_PRICE_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("price.write_done", exchange=exchange, bar_date=bar_date, rows=written)
    return written


# ---------------------------------------------------------------------------
# Backfill tasks
# ---------------------------------------------------------------------------


@task(name="load-backfill-pending-symbols")
def load_backfill_pending_symbols(exchange_code: str, from_date: date) -> list[str]:
    """Symbols that still need historical EOD data for the given exchange.

    Returns fully-qualified symbols (e.g. ``["AAPL.US", "MSFT.US"]``).
    A symbol is considered done if any row for it already exists in
    ``bronze.eod_price`` — this makes re-runs safe without requiring
    gap detection. Add a ``fill_gaps`` mode later if needed.

    Source of truth for what *should* be ingested: ``bronze.instrument``
    (latest snapshot for the exchange).
    """
    from core.clients.lake import get_lake_client

    lake = get_lake_client()

    if not lake.table_exists("bronze", "instrument"):
        log.warning("backfill.no_instrument_table", exchange=exchange_code)
        return []

    instrument_q = lake.qualified_name("bronze", "instrument")
    rows = lake.query(
        f"""
        SELECT DISTINCT ticker
        FROM {instrument_q}
        WHERE exchange_code = ?
          AND snapshot_date = (
              SELECT MAX(snapshot_date) FROM {instrument_q} WHERE exchange_code = ?
          )
        """,
        [exchange_code, exchange_code],
    )
    all_codes = {r["ticker"] for r in rows}

    if not all_codes:
        log.info("backfill.no_instrument", exchange=exchange_code)
        return []

    done_codes: set[str] = set()
    if lake.table_exists("bronze", "eod_price"):
        price_q = lake.qualified_name("bronze", "eod_price")
        done_rows = lake.query(
            f"SELECT DISTINCT ticker FROM {price_q} WHERE exchange_code = ? AND data_provider = ?",
            [exchange_code, EOD_PRICE_DATASET.provider],
        )
        done_codes = {ticker_without_exchange(r["ticker"], exchange_code) for r in done_rows}

    pending = sorted(all_codes - done_codes)
    log.info(
        "backfill.pending_loaded",
        exchange=exchange_code,
        total=len(all_codes),
        done=len(done_codes),
        pending=len(pending),
    )
    return [qualified_ticker(code, exchange_code) for code in pending]


@task(
    name="fetch-ticker-eod-history",
    retries=2,
    retry_delay_seconds=30,
    retry_condition_fn=_is_retryable,
)
async def fetch_ticker_eod_history(
    symbol: str,
    from_date: date,
    to_date: date,
) -> list[EODPriceBarRaw]:
    """Fetch full OHLCV history for one instrument.

    One API call regardless of date range length.
    ``symbol`` must be exchange-qualified, e.g. ``AAPL.US``, ``BTC-USD.CC``.
    """
    log.info("backfill.fetch_start", symbol=symbol, from_date=from_date, to_date=to_date)
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        bars = await client.get_eod_price_ticker(symbol, from_date=from_date, to_date=to_date)
    log.info("backfill.fetch_done", symbol=symbol, bars=len(bars))
    return bars


@task(name="write-backfill-eod-batch")
def write_backfill_eod_batch(
    sources: list[BronzeParseResult[EODBar]],
    exchange_code: str,
) -> int:
    """Bulk-insert one batch of per-ticker bars into bronze.eod_price.

    Takes parser-produced Bronze sources so all records from the batch land in
    a single ``executemany`` call rather than one per symbol.
    """
    from core.clients.lake import get_lake_client

    if not sources:
        return 0

    lake = get_lake_client()
    written = EOD_PRICE_DATASET.write_bronze(lake, sources)
    log.info(
        "backfill.batch_written",
        exchange=exchange_code,
        rows=written,
    )
    return written
