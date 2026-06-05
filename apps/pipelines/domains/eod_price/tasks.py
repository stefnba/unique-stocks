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
    COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED,
    ingestion_coverage_recorded,
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
    task_run_name="fetch-eod-price-bulk-{provider_exchange_code}",
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


@task(
    name="write-eod-price-landing",
    task_run_name="write-eod-price-landing-{provider_exchange_code}",
)
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


@task(
    name="write-ticker-eod-history-landing",
    task_run_name="write-ticker-eod-history-landing-{symbol}",
)
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


@task(
    name="parse-eod-price",
    task_run_name="parse-eod-price-{provider_exchange_code}",
)
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


@task(
    name="write-bronze-eod-price",
    task_run_name="write-bronze-eod-price-{provider_exchange_code}",
)
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


@task(
    name="eod-price-already-ingested",
    task_run_name="eod-price-already-ingested-{provider_exchange_code}",
)
def eod_price_already_ingested(provider_exchange_code: str, bar_date: date) -> bool:
    """Return True when daily EOD data already exists for this exchange/date."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    if not lake.table_exists("bronze", EOD_PRICE_DATASET.table_name):
        return False
    return EOD_PRICE_DATASET.already_ingested(
        lake,
        provider_exchange_code=provider_exchange_code,
        bar_date=bar_date,
    )


# ---------------------------------------------------------------------------
# Backfill tasks
# ---------------------------------------------------------------------------


@task(
    name="load-backfill-pending-symbols",
    task_run_name="load-backfill-pending-symbols-{provider_exchange_code}",
)
def load_backfill_pending_symbols(provider_exchange_code: str, from_date: date, to_date: date) -> list[str]:
    """Symbols that still need historical EOD data for the given exchange.

    Returns fully-qualified symbols (e.g. ``["AAPL.US", "MSFT.US"]``).
    A symbol is excluded from pending when either:

    - ``silver.int_eod_price_backfill_symbol_status`` spans the requested date range, or
    - ``silver.int_eod_price_backfill_no_data_coverage`` has the exact range.

    Coverage rows are generic pipeline metadata (not bronze market data). dbt parses
    the EOD no-data subset into a Silver ingestion-control view so Python does not
    read coverage JSON directly. Delete coverage or price rows and rebuild the
    Silver views to force a retry. The ``no_data`` coverage match includes both
    ``from_date`` and ``to_date`` because a wider later backfill can become valid.

    Source of truth for what *should* be ingested:
    ``silver.int_eod_price_backfill_symbol_status``.
    """
    from core.clients.lake import get_lake_client
    from domains.instrument.universe import (
        EOD_PRICE_BACKFILL_NO_DATA_COVERAGE_TABLE,
        EOD_PRICE_BACKFILL_SYMBOL_STATUS_TABLE,
        require_silver_ingestion_model,
    )

    lake = get_lake_client()
    symbol_status_q = require_silver_ingestion_model(
        lake,
        EOD_PRICE_BACKFILL_SYMBOL_STATUS_TABLE,
        build_hint="dbt-build/price-build",
    )
    no_data_coverage_q = require_silver_ingestion_model(
        lake,
        EOD_PRICE_BACKFILL_NO_DATA_COVERAGE_TABLE,
        build_hint="dbt-build/price-build",
    )

    rows = lake.query(
        f"""
        WITH scoped_symbols AS (
            SELECT
                status.provider_symbol,
                COALESCE(status.min_bar_date <= ? AND status.max_bar_date >= ?, FALSE) AS is_done,
                coverage.provider_symbol IS NOT NULL AS is_covered
            FROM {symbol_status_q} AS status
            LEFT JOIN {no_data_coverage_q} AS coverage
                ON coverage.data_provider = status.data_provider
                AND coverage.provider_exchange_code = status.provider_exchange_code
                AND coverage.provider_symbol = status.provider_symbol
                AND coverage.from_date = ?
                AND coverage.to_date = ?
            WHERE status.data_provider = ?
              AND status.provider_exchange_code = ?
        )
        SELECT
            provider_symbol,
            COUNT(*) OVER () AS total_symbols,
            SUM(CASE WHEN is_done THEN 1 ELSE 0 END) OVER () AS completed_price_symbols,
            SUM(CASE WHEN is_covered THEN 1 ELSE 0 END) OVER () AS no_data_coverage_symbols
        FROM scoped_symbols
        WHERE NOT is_done
          AND NOT is_covered
        ORDER BY provider_symbol
        """,
        [
            from_date.isoformat(),
            to_date.isoformat(),
            from_date.isoformat(),
            to_date.isoformat(),
            str(EOD_PRICE_DATASET.provider),
            provider_exchange_code,
        ],
    )
    pending = [str(row["provider_symbol"]) for row in rows]
    stats = rows[0] if rows else {}
    log.info(
        "backfill.pending_loaded",
        provider_exchange_code=provider_exchange_code,
        from_date=from_date.isoformat(),
        to_date=to_date.isoformat(),
        total=stats.get("total_symbols"),
        done=stats.get("completed_price_symbols"),
        covered=stats.get("no_data_coverage_symbols"),
        pending=len(pending),
    )
    return pending


@task(name="load-missing-eod-backfill-selection-views")
def load_missing_eod_backfill_selection_views() -> list[str]:
    """Return required Silver backfill selector views that are missing."""
    from core.clients.lake import get_lake_client
    from domains.instrument.universe import (
        EOD_PRICE_BACKFILL_NO_DATA_COVERAGE_TABLE,
        EOD_PRICE_BACKFILL_SYMBOL_STATUS_TABLE,
        SILVER_SCHEMA,
    )

    lake = get_lake_client()
    required = (
        EOD_PRICE_BACKFILL_SYMBOL_STATUS_TABLE,
        EOD_PRICE_BACKFILL_NO_DATA_COVERAGE_TABLE,
    )
    missing = [table for table in required if not lake.table_exists(SILVER_SCHEMA, table)]
    log.info("backfill.selection_views_checked", missing=missing, ready=not missing)
    return missing


@task(
    name="write-eod-backfill-deferred-coverage",
    task_run_name="write-eod-backfill-deferred-coverage-{provider_exchange_code}",
)
def write_eod_backfill_deferred_coverage(
    *,
    run_id: str,
    provider_exchange_code: str,
    tickers: list[str],
    from_date: date,
    to_date: date,
    reason: str,
) -> BronzeWrite:
    """Record unsubmitted EOD backfill units deferred by provider quota controls."""
    from core.clients.lake import get_lake_client

    if not tickers:
        return BronzeWrite(rows_written=0, reason="no_tickers")

    lake = get_lake_client()
    written = 0
    recorded_at = datetime.now(UTC)
    for ticker in tickers:
        unit_key = eod_ticker_backfill_unit_key(
            provider_exchange_code=provider_exchange_code,
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
        )
        written += record_ingestion_coverage(
            lake,
            run_id=run_id,
            domain=EOD_PRICE_DOMAIN,
            provider=eod_provider(),
            unit_type=EOD_TICKER_BACKFILL_UNIT_TYPE,
            unit_key=unit_key,
            status=COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED,
            reason=reason,
            recorded_at=recorded_at,
        )

    log.info(
        "backfill.deferred_coverage_written",
        run_id=run_id,
        provider_exchange_code=provider_exchange_code,
        tickers=len(tickers),
        rows=written,
        reason=reason,
    )
    return BronzeWrite(rows_written=written, reason=reason if written == 0 else None)


@task(
    name="write-eod-backfill-coverage",
    task_run_name="write-eod-backfill-coverage-{ticker}",
)
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
    task_run_name="fetch-ticker-eod-history-{symbol}",
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


@task(
    name="write-backfill-eod-batch",
    task_run_name="write-backfill-eod-batch-{provider_exchange_code}",
)
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
