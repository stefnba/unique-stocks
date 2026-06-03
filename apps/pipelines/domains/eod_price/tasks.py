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
from core.clients.http.base import ProviderRateLimitError
from core.ingestion import BronzeParseResult, BronzeWrite, LandingWrite
from core.ingestion.coverage import (
    COVERAGE_STATUS_NO_DATA,
    INGESTION_COVERAGE_TABLE_NAME,
    ingestion_coverage_recorded,
    list_ingestion_coverage_unit_keys,
    record_ingestion_coverage,
)
from providers.eodhd.client import EODHDClient
from providers.eodhd.models import EODBulkPriceRaw, EODPriceBarRaw

from .coverage import (
    EOD_PRICE_DOMAIN,
    EOD_TICKER_BACKFILL_UNIT_TYPE,
    NO_VALID_ROWS_COVERAGE_REASON,
    eod_provider,
    eod_ticker_backfill_unit_key,
)
from .datasets import EOD_PRICE_DATASET
from .models import EODBar
from .parsers import parse_eod_bars
from .symbols import qualified_ticker, ticker_without_exchange

log = structlog.get_logger(__name__)


@task(name="fetch-eod-provider-exchange-codes")
async def fetch_eod_provider_exchange_codes() -> list[str]:
    """Provider request codes eligible for bulk EOD ingestion.

    Loads provider catalog/API codes and curated provider namespaces from the
    dbt-built exchange ingestion universe.
    """
    from domains.exchange.provider_universe import load_provider_exchange_codes

    codes = load_provider_exchange_codes("eodhd", purpose="eod_price")
    log.info("price.provider_exchange_codes_loaded", count=len(codes))
    return codes


def _is_retryable(task: object, task_run: TaskRun, state: State) -> bool:
    """Retry transient errors only; never retry schema drift or provider quota exhaustion."""
    exc = state.result(raise_on_failure=False)
    return not isinstance(exc, ValidationError | ProviderRateLimitError)


@task(
    name="fetch-eod-price-bulk",
    retries=3,
    retry_delay_seconds=exponential_backoff(10),
    retry_condition_fn=_is_retryable,
)
async def fetch_eod_price_bulk(
    provider_exchange_code: str,
    bar_date: date | None = None,
) -> list[EODBulkPriceRaw]:
    """Fetch and schema-validate raw EOD price rows for an entire exchange.

    Uses the provider-specific exchange code in the bulk endpoint. If
    ``bar_date`` is omitted, the provider returns its latest available trading day for
    that code.
    Raises ValidationError if the provider response shape doesn't match EODBulkPriceRaw.
    """
    log.info("price.fetch_start", provider_exchange_code=provider_exchange_code, bar_date=bar_date)
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        rows = await client.get_eod_price_bulk(provider_exchange_code=provider_exchange_code, bar_date=bar_date)
    log.info("price.fetch_done", provider_exchange_code=provider_exchange_code, bar_date=bar_date, rows=len(rows))
    return rows


@task(name="write-eod-price-landing")
async def write_eod_price_to_landing(
    raw_rows: list[EODBulkPriceRaw],
    provider_exchange_code: str,
    bar_date: date,
    ingested_at: datetime | None = None,
) -> LandingWrite:
    """Write raw bulk price rows for one exchange to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = EOD_PRICE_DATASET.landings.daily.save(
        s3,
        provider=EOD_PRICE_DATASET.provider,
        data=raw_rows,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "bar_date": bar_date,
        },
        ingested_at=stamp,
    )
    log.info("price.landing_written", provider_exchange_code=provider_exchange_code, bar_date=bar_date, uri=ref.uri)
    return EOD_PRICE_DATASET.landings.daily.landing_write(
        ref,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "bar_date": bar_date,
        },
        rows_raw=len(raw_rows),
    )


@task(name="write-ticker-eod-history-landing")
async def write_ticker_eod_history_to_landing(
    raw_bars: list[EODPriceBarRaw],
    symbol: str,
    provider_exchange_code: str,
    from_date: date,
    to_date: date,
    ingested_at: datetime | None = None,
) -> LandingWrite:
    """Write raw per-ticker historical bars to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = EOD_PRICE_DATASET.landings.backfill.save(
        s3,
        provider=EOD_PRICE_DATASET.provider,
        data=raw_bars,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "ticker": symbol,
            "from_date": from_date,
            "to_date": to_date,
        },
        ingested_at=stamp,
    )
    log.info("backfill.landing_written", symbol=symbol, uri=ref.uri)
    return EOD_PRICE_DATASET.landings.backfill.landing_write(
        ref,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "ticker": symbol,
            "from_date": from_date,
            "to_date": to_date,
        },
        rows_raw=len(raw_bars),
    )


@task(name="parse-eod-price")
def parse_eod_price(
    raw_rows: list[EODBulkPriceRaw],
    bar_date: date,
    provider_exchange_code: str,
) -> tuple[list[BronzeParseResult[EODBar]], list[EODBulkPriceRaw]]:
    """Apply business validation and convert raw rows to EODBar domain models.

    Logs and drops invalid rows; a few bad tickers should not abort an
    entire exchange's worth of data.
    """
    valid, rejected = parse_eod_bars(
        raw_rows,
        expected_date=bar_date,
        provider_exchange_code=provider_exchange_code,
    )
    if rejected:
        log.warning(
            "price.parse_rejections",
            count=len(rejected),
            bar_date=bar_date,
            provider_exchange_code=provider_exchange_code,
        )
    log.info(
        "price.parsed",
        valid=len(valid),
        rejected=len(rejected),
        bar_date=bar_date,
        provider_exchange_code=provider_exchange_code,
    )
    return valid, rejected


@task(name="write-bronze-eod-price")
def write_bronze_eod_price(
    sources: list[BronzeParseResult[EODBar]],
    provider_exchange_code: str,
    bar_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write validated bars to bronze.eod_price.

    Idempotency is checked at the (provider_exchange_code, bar_date) level; the
    ``data_provider`` column is enforced in ``already_ingested``. Re-running the
    flow for the same exchange + date is safe.
    """
    from core.clients.lake import get_lake_client

    if not sources:
        log.info(
            "price.write_skipped", reason="no_bars", provider_exchange_code=provider_exchange_code, bar_date=bar_date
        )
        return BronzeWrite(rows_written=0, reason="no_bars")

    lake = get_lake_client()
    if EOD_PRICE_DATASET.already_ingested(
        lake,
        provider_exchange_code=provider_exchange_code,
        bar_date=bar_date,
    ):
        log.info(
            "price.write_skipped",
            reason="already_ingested",
            provider_exchange_code=provider_exchange_code,
            bar_date=bar_date,
        )
        return BronzeWrite(rows_written=0, reason="already_ingested")

    written = EOD_PRICE_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("price.write_done", provider_exchange_code=provider_exchange_code, bar_date=bar_date, rows=written)
    return BronzeWrite(rows_written=written)


# ---------------------------------------------------------------------------
# Backfill tasks
# ---------------------------------------------------------------------------


@task(name="load-backfill-pending-symbols")
def load_backfill_pending_symbols(provider_exchange_code: str, from_date: date, to_date: date) -> list[str]:
    """Symbols that still need historical EOD data for the given exchange.

    Returns fully-qualified symbols (e.g. ``["AAPL.US", "MSFT.US"]``).
    A symbol is excluded from pending when either:

    - Any row for it exists in ``bronze.eod_price`` for this exchange, or
    - A ``no_data`` row exists in ``pipeline.ingestion_coverage`` for the same
      ticker backfill unit key (see ``domains/eod_price/coverage.py``).

    Coverage rows are generic pipeline metadata (not bronze market data). They record
    terminal outcomes so re-runs skip completed partitions. Delete coverage or price
    rows to force a retry. The ``no_data`` coverage match includes both
    ``from_date`` and ``to_date`` because a wider later backfill can become valid.

    Source of truth for what *should* be ingested: ``bronze.instrument``
    (latest snapshot for the exchange).
    """
    from core.clients.lake import get_lake_client

    lake = get_lake_client()

    if not lake.table_exists("bronze", "instrument"):
        log.warning("backfill.no_instrument_table", provider_exchange_code=provider_exchange_code)
        return []

    instrument_q = lake.qualified_name("bronze", "instrument")
    rows = lake.query(
        f"""
        SELECT DISTINCT ticker
        FROM {instrument_q}
        WHERE provider_exchange_code = ?
          AND snapshot_date = (
              SELECT MAX(snapshot_date) FROM {instrument_q} WHERE provider_exchange_code = ?
          )
        """,
        [provider_exchange_code, provider_exchange_code],
    )
    all_codes = {r["ticker"] for r in rows}

    if not all_codes:
        log.info("backfill.no_instrument", provider_exchange_code=provider_exchange_code)
        return []

    done_codes: set[str] = set()
    if lake.table_exists("bronze", "eod_price"):
        price_q = lake.qualified_name("bronze", "eod_price")
        done_rows = lake.query(
            f"SELECT DISTINCT ticker FROM {price_q} WHERE provider_exchange_code = ? AND data_provider = ?",
            [provider_exchange_code, EOD_PRICE_DATASET.provider],
        )
        done_codes = {ticker_without_exchange(r["ticker"], provider_exchange_code) for r in done_rows}

    covered_codes: set[str] = set()
    if lake.table_exists("pipeline", INGESTION_COVERAGE_TABLE_NAME):
        covered_keys = list_ingestion_coverage_unit_keys(
            lake,
            domain=EOD_PRICE_DOMAIN,
            provider=eod_provider(),
            unit_type=EOD_TICKER_BACKFILL_UNIT_TYPE,
            status=COVERAGE_STATUS_NO_DATA,
            unit_key_matches={
                "provider_exchange_code": provider_exchange_code,
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
            },
        )
        covered_codes = {
            ticker_without_exchange(str(unit_key["ticker"]), provider_exchange_code)
            for unit_key in covered_keys
            if unit_key.get("ticker")
        }

    pending = sorted(all_codes - done_codes - covered_codes)
    log.info(
        "backfill.pending_loaded",
        provider_exchange_code=provider_exchange_code,
        total=len(all_codes),
        done=len(done_codes),
        covered=len(covered_codes),
        pending=len(pending),
    )
    return [qualified_ticker(code, provider_exchange_code) for code in pending]


@task(name="write-eod-backfill-coverage")
def write_eod_backfill_coverage(
    *,
    run_id: str,
    provider_exchange_code: str,
    ticker: str,
    from_date: date,
    to_date: date,
    rows_raw: int,
    rows_valid: int,
    rows_rejected: int,
    source_uri: str,
) -> BronzeWrite:
    """Record a terminal no-price backfill outcome in ``pipeline.ingestion_coverage``.

    Call this only after the provider fetch and landing write succeeded and the
    provider returned zero rows for the exact ticker/date-range unit. Quota
    failures and all-parser-rejected payloads deliberately stay retryable.

    Idempotent on ``(domain, provider, unit_type, unit_key_hash, status)``.
    Links to the parent ``pipeline.runs`` row via ``run_id``.
    """
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    unit_key = eod_ticker_backfill_unit_key(
        provider_exchange_code=provider_exchange_code,
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
    )
    if ingestion_coverage_recorded(
        lake,
        domain=EOD_PRICE_DOMAIN,
        provider=eod_provider(),
        unit_type=EOD_TICKER_BACKFILL_UNIT_TYPE,
        unit_key=unit_key,
        status=COVERAGE_STATUS_NO_DATA,
    ):
        log.info(
            "backfill.coverage_skipped",
            reason="already_recorded",
            provider_exchange_code=provider_exchange_code,
            ticker=ticker,
            from_date=from_date,
        )
        return BronzeWrite(rows_written=0, reason="already_recorded")

    written = record_ingestion_coverage(
        lake,
        run_id=run_id,
        domain=EOD_PRICE_DOMAIN,
        provider=eod_provider(),
        unit_type=EOD_TICKER_BACKFILL_UNIT_TYPE,
        unit_key=unit_key,
        status=COVERAGE_STATUS_NO_DATA,
        reason=NO_VALID_ROWS_COVERAGE_REASON,
        rows_raw=rows_raw,
        rows_valid=rows_valid,
        rows_rejected=rows_rejected,
        source_uri=source_uri,
        recorded_at=datetime.now(UTC),
    )
    log.info(
        "backfill.coverage_written",
        run_id=run_id,
        provider_exchange_code=provider_exchange_code,
        ticker=ticker,
        from_date=from_date,
        status=COVERAGE_STATUS_NO_DATA,
        rows=written,
    )
    return BronzeWrite(rows_written=written)


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
    provider_exchange_code: str,
) -> BronzeWrite:
    """Bulk-insert one batch of per-ticker bars into bronze.eod_price.

    Takes parser-produced Bronze sources so all records from the batch land in
    a single ``executemany`` call rather than one per symbol.
    """
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_sources")

    lake = get_lake_client()
    written = EOD_PRICE_DATASET.write_bronze(lake, sources)
    log.info(
        "backfill.batch_written",
        provider_exchange_code=provider_exchange_code,
        rows=written,
    )
    return BronzeWrite(rows_written=written)
