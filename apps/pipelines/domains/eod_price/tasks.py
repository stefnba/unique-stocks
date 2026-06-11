"""Prefect tasks for EOD price ingestion.

Tasks are atomic, retryable units. Each task does exactly one thing:
fetch, validate, or write. No business logic.
"""

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, TypedDict

import structlog
from prefect import task
from prefect.client.schemas.objects import State, TaskRun
from prefect.tasks import exponential_backoff
from pydantic import ValidationError

from config.blocks import BlockRegistry
from core.clients.http.base import ProviderRateLimitError
from core.ingestion import BronzeParseResult, BronzeWrite, LandingWrite
from core.ingestion.coverage import (
    COVERAGE_STATUS_COMPLETED,
    COVERAGE_STATUS_NO_DATA,
    COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED,
    ingestion_coverage_recorded,
    record_ingestion_coverage,
)
from providers.eodhd.client import EODHDClient
from providers.eodhd.identifiers import eodhd_api_symbol
from providers.eodhd.models import EODBulkPriceRaw, EODPriceBarRaw

from .coverage import (
    EOD_INSTRUMENT_BACKFILL_UNIT_TYPE,
    EOD_PRICE_DOMAIN,
    NO_VALID_ROWS_COVERAGE_REASON,
    PRICE_ROWS_COMPLETED_COVERAGE_REASON,
    eod_instrument_backfill_unit_key,
    eod_provider,
)
from .datasets import EOD_PRICE_DATASET
from .models import EODBar
from .parsers import parse_eod_bars

log = structlog.get_logger(__name__)
_SQL_DIR = Path(__file__).with_name("sql")
_FULL_HISTORY_LANDING_FROM_DATE = "all"


class EODBackfillCoverageOutcome(TypedDict):
    """Successful per-instrument backfill outcome to record after Bronze writes."""

    provider_instrument_code: str
    rows_raw: int
    rows_valid: int
    rows_rejected: int
    source_uri: str


class EODPriceCoverageGap(TypedDict):
    """Exchange/day price coverage gap returned by the dbt control surface."""

    data_provider: str
    provider_exchange_code: str
    bar_date: date
    exchange_day_status: str
    expected_instruments: int
    priced_instruments: int
    missing_price_instruments: int
    known_no_data_instruments: int
    unknown_calendar_instruments: int


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
    name="write-instrument-eod-history-landing",
    task_run_name="write-instrument-eod-history-landing-{provider_exchange_code}-{provider_instrument_code}",
)
async def write_instrument_eod_history_to_landing(
    raw_bars: list[EODPriceBarRaw],
    provider_exchange_code: str,
    provider_instrument_code: str,
    from_date: date | None,
    to_date: date,
    ingested_at: datetime | None = None,
) -> LandingWrite:
    """Write raw per-instrument historical bars to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = EOD_PRICE_DATASET.landings.backfill.save(
        s3,
        provider=EOD_PRICE_DATASET.provider,
        data=raw_bars,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "provider_instrument_code": provider_instrument_code,
            "from_date": from_date or _FULL_HISTORY_LANDING_FROM_DATE,
            "to_date": to_date,
        },
        ingested_at=stamp,
    )
    log.info(
        "backfill.landing_written",
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        uri=ref.uri,
    )
    return EOD_PRICE_DATASET.landings.backfill.landing_write(
        ref,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "provider_instrument_code": provider_instrument_code,
            "from_date": from_date or _FULL_HISTORY_LANDING_FROM_DATE,
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

    Logs and drops invalid rows; a few bad instruments should not abort an
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

    Daily pre-fetch idempotency is checked before this task. The writer always
    attempts inserts and relies on the Bronze unique key so partial exchange/date
    repair reruns can add missing provider instruments.
    """
    from core.clients.lake import get_lake_client

    if not sources:
        log.info(
            "price.write_skipped", reason="no_bars", provider_exchange_code=provider_exchange_code, bar_date=bar_date
        )
        return BronzeWrite(rows_written=0, reason="no_bars")

    lake = get_lake_client()
    records, duplicates = _deduplicate_eod_price_records(sources, source_uri=source_uri)
    written = _insert_eod_price_records_ignore_existing(lake, records)
    log.info(
        "price.write_done",
        provider_exchange_code=provider_exchange_code,
        bar_date=bar_date,
        rows=written,
        duplicates_dropped=duplicates,
    )
    reason = "already_ingested" if written == 0 and records else None
    return BronzeWrite(rows_written=written, reason=reason)


@task(
    name="eod-price-already-ingested",
    task_run_name="eod-price-already-ingested-{provider_exchange_code}",
)
def eod_price_already_ingested(provider_exchange_code: str, bar_date: date) -> bool:
    """Return True when daily bulk EOD data already exists for this exchange/date."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    if not _eod_daily_bulk_already_ingested(lake, provider_exchange_code=provider_exchange_code, bar_date=bar_date):
        return False
    return not _eod_exchange_day_has_blocking_gap(
        lake,
        provider_exchange_code=provider_exchange_code,
        bar_date=bar_date,
    )


# ---------------------------------------------------------------------------
# Backfill tasks
# ---------------------------------------------------------------------------


@task(
    name="load-backfill-pending-instruments",
    task_run_name="load-backfill-pending-instruments-{provider_exchange_code}",
)
def load_backfill_pending_instruments(provider_exchange_code: str, from_date: date | None, to_date: date) -> list[str]:
    """Provider instrument codes that still need historical EOD data for the exchange/window.

    Exact terminal coverage still wins: completed/no-data coverage for the exact
    requested window removes an instrument from pending.

    Explicit windows use ``silver.int_eod_price_instrument_day_coverage`` so a
    first/last price span with holes in the middle remains pending. Open-start
    full-history windows still require terminal completed/no-data coverage because
    staged prices alone cannot prove provider-earliest history was requested. Instruments
    with no observed price range also require exact terminal coverage; partial no-data
    windows do not satisfy a wider explicit request.

    Coverage rows are generic pipeline metadata (not bronze market data). dbt parses
    the EOD terminal subset into a Silver ingestion-control view so Python does not
    read coverage JSON directly. Delete coverage or price rows and rebuild the
    Silver views to force a retry. Coverage matches include both ``from_date``
    and ``to_date`` because a wider later backfill can become valid.

    Source of truth for price holes:
    ``silver.int_eod_price_instrument_day_coverage``.
    """
    from core.clients.lake import get_lake_client
    from domains.instrument.universe import (
        EOD_PRICE_BACKFILL_TERMINAL_COVERAGE_TABLE,
        EOD_PRICE_EXCHANGE_TRADING_DAY_TABLE,
        EOD_PRICE_INSTRUMENT_DAY_COVERAGE_TABLE,
        INSTRUMENT_UNIVERSE_TABLE,
        require_silver_ingestion_model,
    )

    lake = get_lake_client()
    instrument_universe_q = require_silver_ingestion_model(
        lake, INSTRUMENT_UNIVERSE_TABLE, build_hint="instrument-build"
    )
    coverage_q = require_silver_ingestion_model(
        lake, EOD_PRICE_INSTRUMENT_DAY_COVERAGE_TABLE, build_hint="dbt-build/price-build"
    )
    trading_day_q = require_silver_ingestion_model(
        lake, EOD_PRICE_EXCHANGE_TRADING_DAY_TABLE, build_hint="dbt-build/exchange-build"
    )
    terminal_coverage_q = require_silver_ingestion_model(
        lake,
        EOD_PRICE_BACKFILL_TERMINAL_COVERAGE_TABLE,
        build_hint="dbt-build/price-build",
    )
    from_date_param = from_date.isoformat() if from_date else None
    to_date_param = to_date.isoformat()

    rows = lake.query_file(
        _SQL_DIR / "load_backfill_pending_instruments.sql",
        [
            str(EOD_PRICE_DATASET.provider),
            provider_exchange_code,
            str(EOD_PRICE_DATASET.provider),
            provider_exchange_code,
            to_date_param,
            from_date_param,
            from_date_param,
            str(EOD_PRICE_DATASET.provider),
            provider_exchange_code,
            to_date_param,
            from_date_param,
            from_date_param,
            str(EOD_PRICE_DATASET.provider),
            provider_exchange_code,
            from_date_param,
            from_date_param,
            to_date_param,
            from_date_param,
        ],
        template_context={
            "instrument_universe_relation": instrument_universe_q,
            "trading_day_relation": trading_day_q,
            "instrument_day_coverage_relation": coverage_q,
            "terminal_coverage_relation": terminal_coverage_q,
        },
    )
    pending = [str(row["provider_instrument_code"]) for row in rows]
    stats = rows[0] if rows else {}
    log.info(
        "backfill.pending_loaded",
        provider_exchange_code=provider_exchange_code,
        from_date=from_date_param,
        to_date=to_date_param,
        total=stats.get("total_instruments"),
        done=stats.get("completed_coverage_instruments"),
        covered=stats.get("terminal_no_data_instruments"),
        pending=len(pending),
    )
    return pending


@task(name="load-missing-eod-backfill-selection-views")
def load_missing_eod_backfill_selection_views() -> list[str]:
    """Return required Silver backfill selector views that are missing."""
    from core.clients.lake import get_lake_client
    from domains.instrument.universe import (
        EOD_PRICE_BACKFILL_TERMINAL_COVERAGE_TABLE,
        EOD_PRICE_EXCHANGE_TRADING_DAY_TABLE,
        EOD_PRICE_INSTRUMENT_DAY_COVERAGE_TABLE,
        INSTRUMENT_UNIVERSE_TABLE,
        SILVER_SCHEMA,
    )

    lake = get_lake_client()
    required = (
        INSTRUMENT_UNIVERSE_TABLE,
        EOD_PRICE_EXCHANGE_TRADING_DAY_TABLE,
        EOD_PRICE_INSTRUMENT_DAY_COVERAGE_TABLE,
        EOD_PRICE_BACKFILL_TERMINAL_COVERAGE_TABLE,
    )
    missing = [table for table in required if not lake.table_exists(SILVER_SCHEMA, table)]
    log.info("backfill.selection_views_checked", missing=missing, ready=not missing)
    return missing


@task(name="load-eod-price-coverage-gaps")
def load_eod_price_coverage_gaps(
    provider_exchange_codes: list[str],
    from_date: date | None,
    to_date: date | None,
    exchange_dates: dict[str, date] | None = None,
) -> list[EODPriceCoverageGap]:
    """Return exchange/day gaps from the dbt price coverage control surface."""
    from core.clients.lake import get_lake_client
    from domains.instrument.universe import (
        EOD_PRICE_EXCHANGE_DAY_STATUS_TABLE,
        require_silver_ingestion_model,
    )

    lake = get_lake_client()
    status_q = require_silver_ingestion_model(
        lake, EOD_PRICE_EXCHANGE_DAY_STATUS_TABLE, build_hint="dbt-build/price-build"
    )
    rows = _query_exchange_day_coverage_gaps(
        lake,
        status_q=status_q,
        provider_exchange_codes=provider_exchange_codes,
        from_date=from_date,
        to_date=to_date,
        exchange_dates=exchange_dates,
    )
    gaps: list[EODPriceCoverageGap] = [
        {
            "data_provider": str(row["data_provider"]),
            "provider_exchange_code": str(row["provider_exchange_code"]),
            "bar_date": row["bar_date"],
            "exchange_day_status": str(row["exchange_day_status"]),
            "expected_instruments": int(row["expected_instruments"]),
            "priced_instruments": int(row["priced_instruments"]),
            "missing_price_instruments": int(row["missing_price_instruments"]),
            "known_no_data_instruments": int(row["known_no_data_instruments"]),
            "unknown_calendar_instruments": int(row["unknown_calendar_instruments"]),
        }
        for row in rows
    ]
    log.info("price.coverage_gaps_loaded", gaps=len(gaps), exchange=len(provider_exchange_codes))
    return gaps


@task(name="load-eod-latest-expected-exchange-dates")
def load_eod_latest_expected_exchange_dates(
    provider_exchange_codes: list[str],
    as_of_date: date,
) -> dict[str, date]:
    """Return the expected latest EOD date per exchange from the trading-day control surface."""
    from core.clients.lake import get_lake_client
    from domains.instrument.universe import (
        EOD_PRICE_EXCHANGE_TRADING_DAY_TABLE,
        require_silver_ingestion_model,
    )

    codes = sorted(set(provider_exchange_codes))
    if not codes:
        return {}

    lake = get_lake_client()
    trading_day_q = require_silver_ingestion_model(
        lake,
        EOD_PRICE_EXCHANGE_TRADING_DAY_TABLE,
        build_hint="dbt-build/exchange-build",
    )
    code_placeholders = ", ".join("?" for _ in codes)
    rows = lake.query_file(
        _SQL_DIR / "load_latest_expected_exchange_dates.sql",
        [str(EOD_PRICE_DATASET.provider), *codes, as_of_date.isoformat()],
        template_context={
            "trading_day_relation": trading_day_q,
            "code_placeholders": code_placeholders,
        },
    )
    expected_dates = {
        str(row["provider_exchange_code"]): _coerce_date(row["latest_expected_bar_date"])
        for row in rows
        if row["latest_expected_bar_date"] is not None
    }
    log.info("price.latest_expected_dates_loaded", exchanges=len(expected_dates), as_of_date=as_of_date)
    return expected_dates


@task(
    name="write-eod-backfill-deferred-coverage",
    task_run_name="write-eod-backfill-deferred-coverage-{provider_exchange_code}",
)
def write_eod_backfill_deferred_coverage(
    *,
    run_id: str,
    provider_exchange_code: str,
    provider_instrument_codes: list[str],
    from_date: date | None,
    to_date: date,
    reason: str,
) -> BronzeWrite:
    """Record unsubmitted EOD backfill units deferred by provider quota controls."""
    from core.clients.lake import get_lake_client

    if not provider_instrument_codes:
        return BronzeWrite(rows_written=0, reason="no_instruments")

    lake = get_lake_client()
    written = 0
    recorded_at = datetime.now(UTC)
    for provider_instrument_code in provider_instrument_codes:
        unit_key = eod_instrument_backfill_unit_key(
            provider_exchange_code=provider_exchange_code,
            provider_instrument_code=provider_instrument_code,
            from_date=from_date,
            to_date=to_date,
        )
        written += record_ingestion_coverage(
            lake,
            run_id=run_id,
            domain=EOD_PRICE_DOMAIN,
            provider=eod_provider(),
            unit_type=EOD_INSTRUMENT_BACKFILL_UNIT_TYPE,
            unit_key=unit_key,
            status=COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED,
            reason=reason,
            recorded_at=recorded_at,
        )

    log.info(
        "backfill.deferred_coverage_written",
        run_id=run_id,
        provider_exchange_code=provider_exchange_code,
        instruments=len(provider_instrument_codes),
        rows=written,
        reason=reason,
    )
    return BronzeWrite(rows_written=written, reason=reason if written == 0 else None)


@task(
    name="write-eod-backfill-coverage",
    task_run_name="write-eod-backfill-coverage-{provider_exchange_code}-{provider_instrument_code}",
)
def write_eod_backfill_coverage(
    *,
    run_id: str,
    provider_exchange_code: str,
    provider_instrument_code: str,
    from_date: date | None,
    to_date: date,
    rows_raw: int,
    rows_valid: int,
    rows_rejected: int,
    source_uri: str,
) -> BronzeWrite:
    """Record a terminal no-price backfill outcome in ``pipeline.ingestion_coverage``.

    Call this only after the provider fetch and landing write succeeded and the
    provider returned zero rows for the exact instrument/date-range unit. Quota
    failures and all-parser-rejected payloads deliberately stay retryable.

    Idempotent on ``(domain, provider, unit_type, unit_key_hash, status)``.
    Links to the parent ``pipeline.runs`` row via ``run_id``.
    """
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    unit_key = eod_instrument_backfill_unit_key(
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        from_date=from_date,
        to_date=to_date,
    )
    if ingestion_coverage_recorded(
        lake,
        domain=EOD_PRICE_DOMAIN,
        provider=eod_provider(),
        unit_type=EOD_INSTRUMENT_BACKFILL_UNIT_TYPE,
        unit_key=unit_key,
        status=COVERAGE_STATUS_NO_DATA,
    ):
        log.info(
            "backfill.coverage_skipped",
            reason="already_recorded",
            provider_exchange_code=provider_exchange_code,
            provider_instrument_code=provider_instrument_code,
            from_date=from_date,
        )
        return BronzeWrite(rows_written=0, reason="already_recorded")

    written = record_ingestion_coverage(
        lake,
        run_id=run_id,
        domain=EOD_PRICE_DOMAIN,
        provider=eod_provider(),
        unit_type=EOD_INSTRUMENT_BACKFILL_UNIT_TYPE,
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
        provider_instrument_code=provider_instrument_code,
        from_date=from_date,
        status=COVERAGE_STATUS_NO_DATA,
        rows=written,
    )
    return BronzeWrite(rows_written=written)


@task(
    name="write-eod-backfill-completed-coverage",
    task_run_name="write-eod-backfill-completed-coverage-{provider_exchange_code}",
)
def write_eod_backfill_completed_coverage(
    *,
    run_id: str,
    provider_exchange_code: str,
    from_date: date | None,
    to_date: date,
    outcomes: list[EODBackfillCoverageOutcome],
) -> BronzeWrite:
    """Record completed per-instrument backfill outcomes for exact-window resume.

    Bronze rows remain the source of market data truth. This coverage row is
    orchestration metadata that lets an open-start/full-history backfill skip a
    provider instrument on re-run without mistaking a daily one-off bar for full history.
    """
    from core.clients.lake import get_lake_client

    if not outcomes:
        return BronzeWrite(rows_written=0, reason="no_outcomes")

    lake = get_lake_client()
    written = 0
    recorded_at = datetime.now(UTC)
    for outcome in outcomes:
        unit_key = eod_instrument_backfill_unit_key(
            provider_exchange_code=provider_exchange_code,
            provider_instrument_code=outcome["provider_instrument_code"],
            from_date=from_date,
            to_date=to_date,
        )
        written += record_ingestion_coverage(
            lake,
            run_id=run_id,
            domain=EOD_PRICE_DOMAIN,
            provider=eod_provider(),
            unit_type=EOD_INSTRUMENT_BACKFILL_UNIT_TYPE,
            unit_key=unit_key,
            status=COVERAGE_STATUS_COMPLETED,
            reason=PRICE_ROWS_COMPLETED_COVERAGE_REASON,
            rows_raw=outcome["rows_raw"],
            rows_valid=outcome["rows_valid"],
            rows_rejected=outcome["rows_rejected"],
            source_uri=outcome["source_uri"],
            recorded_at=recorded_at,
        )

    log.info(
        "backfill.completed_coverage_written",
        run_id=run_id,
        provider_exchange_code=provider_exchange_code,
        outcomes=len(outcomes),
        rows=written,
    )
    return BronzeWrite(rows_written=written, reason="already_recorded" if written == 0 else None)


@task(
    name="fetch-instrument-eod-history",
    task_run_name="fetch-instrument-eod-history-{provider_exchange_code}-{provider_instrument_code}",
    retries=2,
    retry_delay_seconds=30,
    retry_condition_fn=_is_retryable,
)
async def fetch_instrument_eod_history(
    provider_exchange_code: str,
    provider_instrument_code: str,
    from_date: date | None,
    to_date: date,
) -> list[EODPriceBarRaw]:
    """Fetch full OHLCV history for one instrument.

    One API call regardless of date range length.
    If ``from_date`` is omitted, the provider returns all available history
    through ``to_date``.
    """
    api_symbol = eodhd_api_symbol(
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    )
    log.info(
        "backfill.fetch_start",
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        api_symbol=api_symbol,
        from_date=from_date,
        to_date=to_date,
    )
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        bars = await client.get_eod_price_ticker(api_symbol, from_date=from_date, to_date=to_date)
    log.info(
        "backfill.fetch_done",
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        api_symbol=api_symbol,
        bars=len(bars),
    )
    return bars


@task(
    name="write-backfill-eod-batch",
    task_run_name="write-backfill-eod-batch-{provider_exchange_code}",
)
def write_backfill_eod_batch(
    sources: list[BronzeParseResult[EODBar]],
    provider_exchange_code: str,
) -> BronzeWrite:
    """Bulk-insert new per-instrument bars into bronze.eod_price.

    Backfill keeps partially completed instruments pending. A fetched range can
    therefore overlap existing Bronze dates, and some provider payloads can
    contain duplicate dates for the same instrument. The write is idempotent at the
    Bronze unique key ``(provider_exchange_code, provider_instrument_code, bar_date, data_provider)``.
    """
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_sources")

    lake = get_lake_client()
    records, duplicates = _deduplicate_eod_price_records(sources)
    written = _insert_eod_price_records_ignore_existing(lake, records)
    log.info(
        "backfill.batch_written",
        provider_exchange_code=provider_exchange_code,
        rows=written,
        duplicates_dropped=duplicates,
        sources=len(sources),
    )
    reason = "already_ingested" if written == 0 and records else None
    return BronzeWrite(rows_written=written, reason=reason)


def _deduplicate_eod_price_records(
    sources: list[BronzeParseResult[EODBar]],
    *,
    source_uri: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Serialize and de-duplicate EOD price records by Bronze unique key."""
    records_by_key: dict[tuple[str, str, date, str], dict[str, Any]] = {}
    duplicates = 0
    for source in sources:
        record = EOD_PRICE_DATASET.bronze_record(source, source_uri=source_uri)
        key = (
            str(record["provider_exchange_code"]),
            str(record["provider_instrument_code"]),
            record["bar_date"],
            str(record["data_provider"]),
        )
        if key in records_by_key:
            duplicates += 1
            continue
        records_by_key[key] = record
    return list(records_by_key.values()), duplicates


def _coerce_date(value: object) -> date:
    """Return a date from lake query output."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _eod_daily_bulk_already_ingested(lake: Any, *, provider_exchange_code: str, bar_date: date) -> bool:
    """Return whether the daily bulk path already wrote any row for an exchange/date."""
    if not lake.table_exists(EOD_PRICE_DATASET.schema, EOD_PRICE_DATASET.table_name):
        return False

    qualified = lake.qualified_name(EOD_PRICE_DATASET.schema, EOD_PRICE_DATASET.table_name)
    row = lake.query_one(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {qualified}
        WHERE provider_exchange_code = ?
          AND bar_date = ?
          AND data_provider = ?
          AND ingestion_mode = ?
        """,
        [provider_exchange_code, bar_date.isoformat(), str(EOD_PRICE_DATASET.provider), "daily_bulk"],
    )
    return bool(row and row["cnt"] > 0)


def _eod_exchange_day_has_blocking_gap(lake: Any, *, provider_exchange_code: str, bar_date: date) -> bool:
    """Return whether rebuilt coverage says a daily rerun may still repair gaps."""
    status_table = "int_eod_price_exchange_day_status"
    if not lake.table_exists("silver", status_table):
        return False

    qualified = lake.qualified_name("silver", status_table)
    row = lake.query_one(
        f"""
        SELECT exchange_day_status
        FROM {qualified}
        WHERE data_provider = ?
          AND provider_exchange_code = ?
          AND bar_date = ?
        """,
        [str(EOD_PRICE_DATASET.provider), provider_exchange_code, bar_date.isoformat()],
    )
    if not row:
        return False
    return row["exchange_day_status"] in {"missing_price", "unknown_calendar"}


def _query_exchange_day_coverage_gaps(
    lake: Any,
    *,
    status_q: str,
    provider_exchange_codes: list[str],
    from_date: date | None,
    to_date: date | None,
    exchange_dates: dict[str, date] | None,
) -> list[dict[str, Any]]:
    """Query exchange/day coverage rows that require investigation or repair."""
    if exchange_dates is not None:
        pairs = sorted((code, bar_date) for code, bar_date in exchange_dates.items() if bar_date is not None)
        if not pairs:
            return []
        pair_clauses = " OR ".join("(provider_exchange_code = ? AND bar_date = ?)" for _ in pairs)
        params: list[Any] = [str(EOD_PRICE_DATASET.provider)]
        for code, bar_date in pairs:
            params.extend([code, bar_date.isoformat()])
        return lake.query_file(
            _SQL_DIR / "load_exchange_day_coverage_gaps_by_pairs.sql",
            params,
            template_context={
                "status_relation": status_q,
                "pair_predicates": pair_clauses,
            },
        )

    codes = sorted(set(provider_exchange_codes))
    if not codes:
        return []
    code_placeholders = ", ".join("?" for _ in codes)
    params = [str(EOD_PRICE_DATASET.provider), *codes]
    has_from_date_filter = from_date is not None
    has_to_date_filter = to_date is not None
    if from_date is not None:
        params.append(from_date.isoformat())
    if to_date is not None:
        params.append(to_date.isoformat())
    return lake.query_file(
        _SQL_DIR / "load_exchange_day_coverage_gaps_by_codes.sql",
        params,
        template_context={
            "status_relation": status_q,
            "code_placeholders": code_placeholders,
            "from_date_filter": has_from_date_filter,
            "to_date_filter": has_to_date_filter,
        },
    )


def _insert_eod_price_records_ignore_existing(lake: Any, records: list[dict[str, Any]]) -> int:
    """Insert records, ignoring rows already present by the Bronze unique key."""
    if not records:
        return 0

    if not lake.table_exists(EOD_PRICE_DATASET.schema, EOD_PRICE_DATASET.table_name):
        lake.execute(EOD_PRICE_DATASET.table.to_ddl())

    qualified = lake.qualified_name(EOD_PRICE_DATASET.schema, EOD_PRICE_DATASET.table_name)
    before = _table_count(lake, qualified)
    columns = list(records[0].keys())
    column_names = ", ".join(_quote_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    values = [[record[column] for column in columns] for record in records]
    lake.connection.executemany(f"INSERT OR IGNORE INTO {qualified} ({column_names}) VALUES ({placeholders})", values)
    return _table_count(lake, qualified) - before


def _table_count(lake: Any, qualified: str) -> int:
    """Return the current row count for a qualified table name."""
    row = lake.query_one(f"SELECT COUNT(*) AS cnt FROM {qualified}")
    return int(row["cnt"]) if row else 0


def _quote_identifier(value: str) -> str:
    """Quote a DuckDB identifier."""
    if not value or "\x00" in value:
        raise ValueError(f"Invalid DuckDB identifier: {value!r}")
    return '"' + value.replace('"', '""') + '"'
