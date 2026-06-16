"""Prefect tasks for fundamentals ingestion."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol, cast

import structlog
from prefect import task
from prefect.client.schemas.objects import State, TaskRun
from prefect.tasks import TaskRunNameCallbackWithParameters, exponential_backoff
from pydantic import ValidationError

from control_plane.prefect import BlockRegistry
from core.http.base import ProviderRateLimitError
from core.ingestion import BronzeParseResult, BronzeWrite, LandingWrite
from core.ingestion.coverage import COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED, record_ingestion_coverage
from core.ingestion.keys import ObjectStorageKey
from domains.fundamental.batch import resolve_fundamental_snapshot_date
from domains.fundamental.datasets import (
    FUNDAMENTAL_DOCUMENT_DATASET,
    FUNDAMENTAL_ETF_HOLDING_DATASET,
    FUNDAMENTAL_ETF_IDENTITY_DATASET,
    FUNDAMENTAL_FUND_METRIC_FACT_DATASET,
    FUNDAMENTAL_INDEX_COMPONENT_DATASET,
    FUNDAMENTAL_INDEX_HISTORICAL_COMPONENT_DATASET,
    FUNDAMENTAL_INDEX_IDENTITY_DATASET,
    FUNDAMENTAL_MUTUAL_FUND_HOLDING_DATASET,
    FUNDAMENTAL_MUTUAL_FUND_IDENTITY_DATASET,
    FUNDAMENTAL_STATEMENT_FACT_DATASET,
    FUNDAMENTAL_STOCK_DIVIDEND_COUNT_DATASET,
    FUNDAMENTAL_STOCK_EARNINGS_FACT_DATASET,
    FUNDAMENTAL_STOCK_ESG_ACTIVITY_DATASET,
    FUNDAMENTAL_STOCK_HOLDER_DATASET,
    FUNDAMENTAL_STOCK_IDENTITY_DATASET,
    FUNDAMENTAL_STOCK_INSIDER_TRANSACTION_DATASET,
    FUNDAMENTAL_STOCK_METRIC_FACT_DATASET,
    FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_DATASET,
    FUNDAMENTAL_STOCK_SHARES_STATS_DATASET,
    FUNDAMENTAL_STOCK_SPLITS_DIVIDENDS_DATASET,
)
from domains.fundamental.models import (
    FundamentalDocument,
    FundamentalEtfHolding,
    FundamentalEtfIdentitySnapshot,
    FundamentalFundMetricFact,
    FundamentalIndexComponent,
    FundamentalIndexHistoricalComponent,
    FundamentalIndexIdentitySnapshot,
    FundamentalMutualFundHolding,
    FundamentalMutualFundIdentitySnapshot,
    FundamentalStatementFact,
    FundamentalStockDividendCount,
    FundamentalStockEarningsFact,
    FundamentalStockEsgActivity,
    FundamentalStockHolder,
    FundamentalStockIdentitySnapshot,
    FundamentalStockInsiderTransaction,
    FundamentalStockMetricFact,
    FundamentalStockOutstandingShares,
    FundamentalStockSharesStatsSnapshot,
    FundamentalStockSplitsDividendsSnapshot,
)
from domains.fundamental.parsers import (
    parse_etf_holdings,
    parse_etf_identity_snapshot,
    parse_fund_metric_facts,
    parse_fundamental_document,
    parse_index_components,
    parse_index_identity_snapshot,
    parse_mutual_fund_holdings,
    parse_mutual_fund_identity_snapshot,
    parse_stock_earnings_facts,
    parse_stock_holders,
    parse_stock_identity_snapshot,
    parse_stock_insider_transactions,
    parse_stock_metric_facts,
    parse_stock_outstanding_shares,
    parse_stock_shares_stats_snapshot,
    parse_stock_statement_facts,
)
from domains.instrument.universe import (
    FUNDAMENTAL_DOCUMENT_COMPLETION_TABLE,
    FUNDAMENTAL_INGESTION_UNIVERSE_TABLE,
    require_silver_ingestion_model,
)
from providers.eodhd.identifiers import EODHDInstrumentRef, eodhd_api_symbol
from providers.eodhd.models import FundamentalRaw

log = structlog.get_logger(__name__)
_SQL_DIR = Path(__file__).with_name("sql")

FUNDAMENTAL_DOMAIN = "fundamental"
FUNDAMENTAL_PROVIDER = "eodhd"
FUNDAMENTAL_INSTRUMENT_SNAPSHOT_UNIT_TYPE = "instrument_snapshot"


@dataclass(frozen=True, slots=True)
class FundamentalInstrumentSelection:
    """Instrument selection result plus completion-filter metadata."""

    instruments: list[EODHDInstrumentRef]
    completion_filter_applied: bool


class _LandingStorage(Protocol):
    bucket: str | None

    def list_keys(self, prefix: str = "", *, bucket: str | None = None, max_keys: int | None = None) -> list[str]:
        """Return landing object keys under a prefix."""
        ...


def _is_retryable(task: object, task_run: TaskRun, state: State) -> bool:
    """Retry transient errors only; never retry schema drift or provider quota exhaustion."""
    exc = state.result(raise_on_failure=False)
    return not isinstance(exc, ValidationError | ProviderRateLimitError)


def _instrument_task_run_name(task_name: str) -> TaskRunNameCallbackWithParameters:
    """Build a task-run-name callback for tasks keyed by provider instrument."""

    def name(parameters: dict[str, Any]) -> str:
        provider_exchange_code = parameters.get("provider_exchange_code")
        provider_instrument_code = parameters.get("provider_instrument_code")
        if not provider_instrument_code:
            source = parameters.get("source")
            row = getattr(source, "row", None)
            provider_exchange_code = getattr(row, "provider_exchange_code", provider_exchange_code)
            provider_instrument_code = getattr(row, "provider_instrument_code", None)
        return f"{task_name}-{provider_exchange_code or 'unknown'}-{provider_instrument_code or 'unknown'}"

    return cast(TaskRunNameCallbackWithParameters, name)


def _fundamental_bronze_write(
    *,
    bronze_table: str,
    rows_written: int,
    reason: str | None = None,
    source_uri: str | None = None,
    source: BronzeParseResult[Any] | None = None,
    sources: list[BronzeParseResult[Any]] | None = None,
    provider_instrument_code: str | None = None,
    snapshot_date: date | None = None,
) -> BronzeWrite:
    """Return a Bronze write result for one table-slice write."""
    return BronzeWrite(rows_written=rows_written, reason=reason)


@task(name="fetch-fundamental-provider-exchange-codes")
def fetch_fundamental_provider_exchange_codes() -> list[str]:
    """Load provider request codes approved for automatic fundamentals selection."""
    from domains.exchange.provider_universe import load_provider_exchange_codes

    codes = load_provider_exchange_codes("eodhd", purpose="fundamental")
    log.info("fundamental.provider_exchange_codes_loaded", count=len(codes))
    return codes


def _load_fundamental_instrument_selection(
    provider_exchange_codes: list[str] | None = None,
    limit: int | None = None,
    *,
    snapshot_date: date | None = None,
    skip_completed: bool = False,
) -> FundamentalInstrumentSelection:
    """Load provider instruments from Silver and report whether completion filtering ran."""
    from core.lake import get_lake_client

    lake = get_lake_client()
    universe_q = require_silver_ingestion_model(
        lake,
        FUNDAMENTAL_INGESTION_UNIVERSE_TABLE,
        build_hint="dbt-build/fundamental-build after successful exchange-build and instrument-build",
    )
    completion_q = None
    params: list[object] = [FUNDAMENTAL_PROVIDER]
    has_exchange_filter = bool(provider_exchange_codes)
    exchange_placeholders = ""
    if provider_exchange_codes:
        exchange_placeholders = ", ".join("?" for _ in provider_exchange_codes)
        params.extend(provider_exchange_codes)

    if skip_completed:
        if snapshot_date is None:
            raise ValueError("snapshot_date is required when skip_completed=True")
        completion_q = require_silver_ingestion_model(
            lake,
            FUNDAMENTAL_DOCUMENT_COMPLETION_TABLE,
            build_hint="dbt-build/fundamental-build",
        )
        params.append(snapshot_date.isoformat())

    has_limit_filter = limit is not None
    if limit is not None:
        params.append(max(0, int(limit)))

    rows = lake.query_file(
        _SQL_DIR / "load_instrument_selection.sql",
        params,
        template_context={
            "universe_relation": universe_q,
            "exchange_filter": has_exchange_filter,
            "exchange_placeholders": exchange_placeholders,
            "skip_completed": skip_completed,
            "completion_relation": completion_q or "",
            "limit_filter": has_limit_filter,
        },
    )
    instruments = [
        EODHDInstrumentRef(
            provider_exchange_code=str(row["provider_exchange_code"]),
            provider_instrument_code=str(row["provider_instrument_code"]),
        )
        for row in rows
    ]
    log.info(
        "fundamental.instruments_loaded",
        count=len(instruments),
        skip_completed_requested=skip_completed,
        completion_filter_applied=skip_completed,
    )
    return FundamentalInstrumentSelection(
        instruments=instruments,
        completion_filter_applied=skip_completed,
    )


@task(name="load-fundamental-instrument-selection")
def load_fundamental_instrument_selection(
    provider_exchange_codes: list[str] | None = None,
    limit: int | None = None,
    *,
    snapshot_date: date | None = None,
    skip_completed: bool = False,
) -> FundamentalInstrumentSelection:
    """Load latest provider instruments plus completion-filter metadata."""
    return _load_fundamental_instrument_selection(
        provider_exchange_codes=provider_exchange_codes,
        limit=limit,
        snapshot_date=snapshot_date,
        skip_completed=skip_completed,
    )


@task(name="load-fundamental-instruments")
def load_fundamental_instruments(
    provider_exchange_codes: list[str] | None = None,
    limit: int | None = None,
    *,
    snapshot_date: date | None = None,
    skip_completed: bool = False,
) -> list[EODHDInstrumentRef]:
    """Load latest provider instruments from the Silver ingestion universe.

    If ``skip_completed=True``, this requires
    ``silver.int_fundamental_document_completion`` and anti-joins against it.
    """
    return _load_fundamental_instrument_selection(
        provider_exchange_codes=provider_exchange_codes,
        limit=limit,
        snapshot_date=snapshot_date,
        skip_completed=skip_completed,
    ).instruments


@task(name="write-fundamental-deferred-coverage")
def write_fundamental_deferred_coverage(
    *,
    run_id: str,
    instruments: list[EODHDInstrumentRef],
    snapshot_date: date,
    reason: str,
) -> BronzeWrite:
    """Record unsubmitted fundamentals units deferred by provider quota controls."""
    from core.lake import get_lake_client

    if not instruments:
        return BronzeWrite(rows_written=0, reason="no_instruments")

    lake = get_lake_client()
    written = 0
    recorded_at = datetime.now(UTC)
    for instrument in instruments:
        written += record_ingestion_coverage(
            lake,
            run_id=run_id,
            domain=FUNDAMENTAL_DOMAIN,
            provider=FUNDAMENTAL_PROVIDER,
            unit_type=FUNDAMENTAL_INSTRUMENT_SNAPSHOT_UNIT_TYPE,
            unit_key={
                "provider_exchange_code": instrument.provider_exchange_code,
                "provider_instrument_code": instrument.provider_instrument_code,
                "snapshot_date": snapshot_date.isoformat(),
            },
            status=COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED,
            reason=reason,
            recorded_at=recorded_at,
        )

    log.info(
        "fundamental.deferred_coverage_written",
        run_id=run_id,
        instruments=len(instruments),
        rows=written,
        reason=reason,
    )
    return BronzeWrite(rows_written=written, reason=reason if written == 0 else None)


@task(name="load-latest-fundamental-ingestion-batch-date")
def load_latest_fundamental_ingestion_batch_date() -> date | None:
    """Return the latest ``snapshot_date`` present in ``bronze.fundamental_document``.

    Used to continue a multi-day backfill under the same bronze partition when
    ``continue_ingestion_batch`` is enabled and no explicit batch date was passed.
    """
    from core.lake import get_lake_client

    lake = get_lake_client()
    if not lake.table_exists("bronze", FUNDAMENTAL_DOCUMENT_DATASET.table_name):
        return None

    qualified = lake.qualified_name("bronze", FUNDAMENTAL_DOCUMENT_DATASET.table_name)
    row = lake.query_one(
        f"""
        SELECT MAX(snapshot_date) AS snapshot_date
        FROM {qualified}
        WHERE data_provider = ?
        """,
        [str(FUNDAMENTAL_DOCUMENT_DATASET.provider)],
    )
    if not row or row.get("snapshot_date") is None:
        return None
    latest = row["snapshot_date"]
    if isinstance(latest, date):
        return latest
    return date.fromisoformat(str(latest))


@task(name="resolve-fundamental-snapshot-date")
def resolve_fundamental_snapshot_date_task(
    *,
    snapshot_date: date | None,
    ingestion_batch_date: date | None,
    continue_ingestion_batch: bool,
) -> dict[str, str]:
    """Resolve the effective fundamentals ingestion batch date for this run."""
    latest = load_latest_fundamental_ingestion_batch_date.fn() if continue_ingestion_batch else None
    resolved = resolve_fundamental_snapshot_date(
        snapshot_date=snapshot_date,
        ingestion_batch_date=ingestion_batch_date,
        continue_ingestion_batch=continue_ingestion_batch,
        latest_bronze_snapshot_date=latest,
        default_date=datetime.now(UTC).date(),
    )
    return {
        "snapshot_date": resolved.snapshot_date.isoformat(),
        "source": resolved.source,
    }


@task(
    name="fundamental-document-already-ingested",
    task_run_name="fundamental-document-already-ingested-{provider_exchange_code}-{provider_instrument_code}",
)
def fundamental_document_already_ingested(
    provider_exchange_code: str,
    provider_instrument_code: str,
    snapshot_date: date,
) -> bool:
    """Return True when the fundamentals document already exists for this provider_instrument_code snapshot."""
    from core.lake import get_lake_client

    lake = get_lake_client()
    return FUNDAMENTAL_DOCUMENT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    )


@task(
    name="load-fundamental-document-payload-hash",
    task_run_name="load-fundamental-document-payload-hash-{provider_exchange_code}-{provider_instrument_code}",
)
def load_fundamental_document_payload_hash(
    provider_exchange_code: str,
    provider_instrument_code: str,
    snapshot_date: date,
) -> str | None:
    """Return the stored payload hash for a provider_instrument_code snapshot when one exists."""
    from core.lake import get_lake_client

    lake = get_lake_client()
    if not lake.table_exists("bronze", FUNDAMENTAL_DOCUMENT_DATASET.table_name):
        return None

    qualified = lake.qualified_name("bronze", FUNDAMENTAL_DOCUMENT_DATASET.table_name)
    row = lake.query_one(
        f"""
        SELECT payload_hash
        FROM {qualified}
        WHERE snapshot_date = ?
          AND provider_exchange_code = ?
          AND provider_instrument_code = ?
          AND data_provider = ?
        ORDER BY ingested_at DESC
        LIMIT 1
        """,
        [
            snapshot_date.isoformat(),
            provider_exchange_code,
            provider_instrument_code,
            str(FUNDAMENTAL_DOCUMENT_DATASET.provider),
        ],
    )
    return str(row["payload_hash"]) if row and row.get("payload_hash") else None


@task(
    name="delete-fundamental-snapshot-rows",
    task_run_name="delete-fundamental-snapshot-rows-{provider_exchange_code}-{provider_instrument_code}",
)
def delete_fundamental_snapshot_rows(
    provider_exchange_code: str,
    provider_instrument_code: str,
    snapshot_date: date,
) -> int:
    """Delete existing fundamentals Bronze rows for an explicit provider_instrument_code snapshot.

    Used only by refresh mode after a changed provider document has already
    landed, so the same-day snapshot can be replaced without violating Bronze
    unique constraints.
    """
    from core.lake import get_lake_client

    lake = get_lake_client()
    deleted_tables = 0
    for dataset in (
        FUNDAMENTAL_INDEX_HISTORICAL_COMPONENT_DATASET,
        FUNDAMENTAL_INDEX_COMPONENT_DATASET,
        FUNDAMENTAL_FUND_METRIC_FACT_DATASET,
        FUNDAMENTAL_MUTUAL_FUND_HOLDING_DATASET,
        FUNDAMENTAL_ETF_HOLDING_DATASET,
        FUNDAMENTAL_INDEX_IDENTITY_DATASET,
        FUNDAMENTAL_MUTUAL_FUND_IDENTITY_DATASET,
        FUNDAMENTAL_ETF_IDENTITY_DATASET,
        FUNDAMENTAL_STOCK_ESG_ACTIVITY_DATASET,
        FUNDAMENTAL_STOCK_METRIC_FACT_DATASET,
        FUNDAMENTAL_STOCK_DIVIDEND_COUNT_DATASET,
        FUNDAMENTAL_STOCK_SPLITS_DIVIDENDS_DATASET,
        FUNDAMENTAL_STOCK_INSIDER_TRANSACTION_DATASET,
        FUNDAMENTAL_STOCK_HOLDER_DATASET,
        FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_DATASET,
        FUNDAMENTAL_STOCK_SHARES_STATS_DATASET,
        FUNDAMENTAL_STOCK_EARNINGS_FACT_DATASET,
        FUNDAMENTAL_STATEMENT_FACT_DATASET,
        FUNDAMENTAL_STOCK_IDENTITY_DATASET,
        FUNDAMENTAL_DOCUMENT_DATASET,
    ):
        if not lake.table_exists(dataset.schema, dataset.table_name):
            continue
        lake.execute(
            f"""
            DELETE FROM {lake.qualified_name(dataset.schema, dataset.table_name)}
            WHERE snapshot_date = ?
              AND provider_exchange_code = ?
              AND provider_instrument_code = ?
              AND data_provider = ?
            """,
            [snapshot_date.isoformat(), provider_exchange_code, provider_instrument_code, str(dataset.provider)],
        )
        deleted_tables += 1
    log.info(
        "fundamental.snapshot_deleted",
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
        tables=deleted_tables,
    )
    return deleted_tables


@task(
    name="fetch-fundamental-instrument",
    task_run_name="fetch-fundamental-instrument-{provider_exchange_code}-{provider_instrument_code}",
    retries=3,
    retry_delay_seconds=exponential_backoff(10),
    retry_condition_fn=_is_retryable,
)
async def fetch_fundamental_instrument(provider_exchange_code: str, provider_instrument_code: str) -> FundamentalRaw:
    """Fetch and top-level schema-validate one fundamentals document."""
    from providers.eodhd.client import EODHDClient

    api_symbol = eodhd_api_symbol(
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    )
    log.info(
        "fundamental.fetch_start",
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        api_symbol=api_symbol,
    )
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        raw = await client.get_fundamental(api_symbol)
    log.info(
        "fundamental.fetch_done",
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        api_symbol=api_symbol,
        sections=len(raw.model_dump(exclude_none=True)),
    )
    return raw


@task(
    name="load-fundamental-from-landing",
    task_run_name="load-fundamental-from-landing-{provider_instrument_code}",
)
async def load_fundamental_from_landing(
    provider_exchange_code: str,
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> tuple[FundamentalRaw, LandingWrite]:
    """Load one landed fundamentals document without calling the provider.

    If ``source_uri`` is omitted, the latest landing object for the provider_instrument_code and
    snapshot date is selected by its path-safe ``ingested_at`` partition.
    """
    from core.storage.s3 import S3StorageClient

    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    resolved_uri = source_uri or _latest_fundamental_landing_uri(
        s3,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    payload = s3.load(resolved_uri, format="json")
    raw = FundamentalRaw.model_validate(payload)
    landing = LandingWrite(
        dataset=FUNDAMENTAL_DOCUMENT_DATASET.landings.document.audit_dataset_name,
        source_uri=resolved_uri,
        partition={
            "provider_exchange_code": provider_exchange_code,
            "provider_instrument_code": provider_instrument_code,
            "snapshot_date": snapshot_date,
        },
        rows_raw=1,
    )
    log.info(
        "fundamental.landing_loaded",
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
        uri=resolved_uri,
    )
    return raw, landing


@task(
    name="write-fundamental-landing",
    task_run_name="write-fundamental-landing-{provider_instrument_code}",
)
async def write_fundamental_to_landing(
    raw: FundamentalRaw,
    provider_exchange_code: str,
    provider_instrument_code: str,
    snapshot_date: date,
    ingested_at: datetime | None = None,
) -> LandingWrite:
    """Write one raw fundamentals document to the S3 landing zone as JSON."""
    from core.storage.s3 import S3StorageClient

    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = FUNDAMENTAL_DOCUMENT_DATASET.landings.document.save(
        s3,
        provider=FUNDAMENTAL_DOCUMENT_DATASET.provider,
        data=raw,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "provider_instrument_code": provider_instrument_code,
            "snapshot_date": snapshot_date,
        },
        ingested_at=stamp,
    )
    log.info(
        "fundamental.landing_written",
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
        uri=ref.uri,
    )
    return FUNDAMENTAL_DOCUMENT_DATASET.landings.document.landing_write(
        ref,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "provider_instrument_code": provider_instrument_code,
            "snapshot_date": snapshot_date,
        },
        rows_raw=1,
    )


def _latest_fundamental_landing_uri(
    s3: _LandingStorage,
    *,
    provider_exchange_code: str,
    provider_instrument_code: str,
    snapshot_date: date,
) -> str:
    """Return the newest landed fundamentals object URI for one provider_instrument_code snapshot."""
    key = ObjectStorageKey.partitioned_from_mapping(
        FUNDAMENTAL_DOCUMENT_DATASET.provider,
        FUNDAMENTAL_DOCUMENT_DATASET.landings.document.domain,
        {
            "provider_exchange_code": provider_exchange_code,
            "provider_instrument_code": provider_instrument_code,
            "snapshot_date": snapshot_date,
            "ingested_at": "",
        },
    ).key(FUNDAMENTAL_DOCUMENT_DATASET.landings.document.file_format)
    prefix = key.rsplit("ingested_at=", maxsplit=1)[0] + "ingested_at="
    keys = [
        candidate
        for candidate in s3.list_keys(prefix=prefix)
        if candidate.endswith(f".{FUNDAMENTAL_DOCUMENT_DATASET.landings.document.file_format}")
    ]
    if not keys:
        raise FileNotFoundError(
            "No landed fundamentals document found for "
            f"provider_instrument_code={provider_instrument_code!r}, "
            f"snapshot_date={snapshot_date.isoformat()!r}"
        )
    latest_key = sorted(keys)[-1]
    bucket = _landing_bucket(s3)
    return f"s3://{bucket}/{latest_key}"


def _landing_bucket(s3: _LandingStorage) -> str:
    """Return the storage client's default bucket or raise a replay-friendly error."""
    bucket = getattr(s3, "bucket", None)
    if not bucket:
        raise ValueError("S3 storage client has no default bucket; pass an explicit fundamentals source_uri.")
    return str(bucket)


@task(
    name="parse-fundamental-stock",
    task_run_name="parse-fundamental-stock-{provider_instrument_code}",
)
def parse_fundamental_stock(
    raw: FundamentalRaw,
    provider_exchange_code: str,
    provider_instrument_code: str,
    snapshot_date: date,
) -> tuple[
    BronzeParseResult[FundamentalDocument],
    BronzeParseResult[FundamentalStockIdentitySnapshot] | None,
    list[BronzeParseResult[FundamentalStatementFact]],
    list[BronzeParseResult[FundamentalStockEarningsFact]],
    BronzeParseResult[FundamentalStockSharesStatsSnapshot] | None,
    list[BronzeParseResult[FundamentalStockOutstandingShares]],
    list[BronzeParseResult[FundamentalStockHolder]],
    list[BronzeParseResult[FundamentalStockInsiderTransaction]],
    list[BronzeParseResult[FundamentalStockMetricFact]],
    BronzeParseResult[FundamentalEtfIdentitySnapshot] | None,
    BronzeParseResult[FundamentalMutualFundIdentitySnapshot] | None,
    BronzeParseResult[FundamentalIndexIdentitySnapshot] | None,
    list[BronzeParseResult[FundamentalEtfHolding]],
    list[BronzeParseResult[FundamentalMutualFundHolding]],
    list[BronzeParseResult[FundamentalFundMetricFact]],
    list[BronzeParseResult[FundamentalIndexComponent]],
    list[dict[str, object]],
]:
    """Parse the stock fundamentals slice currently supported by this domain."""
    document = parse_fundamental_document(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    identity = parse_stock_identity_snapshot(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    statement_facts, statement_rejected = parse_stock_statement_facts(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    earnings_facts, earnings_rejected = parse_stock_earnings_facts(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    shares_stats = parse_stock_shares_stats_snapshot(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    outstanding_shares, outstanding_rejected = parse_stock_outstanding_shares(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    holders, holders_rejected = parse_stock_holders(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    insider_transactions, insider_transactions_rejected = parse_stock_insider_transactions(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    metric_facts = parse_stock_metric_facts(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    etf_identity = parse_etf_identity_snapshot(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    mutual_fund_identity = parse_mutual_fund_identity_snapshot(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    index_identity = parse_index_identity_snapshot(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    etf_holdings, etf_rejected = parse_etf_holdings(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    mutual_fund_holdings, mutual_fund_rejected = parse_mutual_fund_holdings(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    fund_metric_facts = parse_fund_metric_facts(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    index_components, index_rejected = parse_index_components(
        raw,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
    )
    rejected: list[dict[str, object]] = [
        *statement_rejected,
        *earnings_rejected,
        *outstanding_rejected,
        *holders_rejected,
        *insider_transactions_rejected,
        *etf_rejected,
        *mutual_fund_rejected,
        *index_rejected,
    ]
    log.info(
        "fundamental.parsed",
        provider_instrument_code=provider_instrument_code,
        family=document.row.instrument_family,
        stock_identity=identity is not None,
        statement_facts=len(statement_facts),
        earnings_facts=len(earnings_facts),
        shares_stats=shares_stats is not None,
        outstanding_shares=len(outstanding_shares),
        holders=len(holders),
        insider_transactions=len(insider_transactions),
        metric_facts=len(metric_facts),
        etf_identity=etf_identity is not None,
        mutual_fund_identity=mutual_fund_identity is not None,
        index_identity=index_identity is not None,
        etf_holdings=len(etf_holdings),
        mutual_fund_holdings=len(mutual_fund_holdings),
        fund_metric_facts=len(fund_metric_facts),
        index_components=len(index_components),
        rejected=len(rejected),
    )
    return (
        document,
        identity,
        statement_facts,
        earnings_facts,
        shares_stats,
        outstanding_shares,
        holders,
        insider_transactions,
        metric_facts,
        etf_identity,
        mutual_fund_identity,
        index_identity,
        etf_holdings,
        mutual_fund_holdings,
        fund_metric_facts,
        index_components,
        rejected,
    )


@task(
    name="write-bronze-fundamental-document",
    task_run_name=_instrument_task_run_name("write-bronze-fundamental-document"),
)
def write_bronze_fundamental_document(
    source: BronzeParseResult[FundamentalDocument],
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write one document metadata row to ``bronze.fundamental_document``."""
    from core.lake import get_lake_client

    lake = get_lake_client()
    if FUNDAMENTAL_DOCUMENT_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        provider_exchange_code=source.row.provider_exchange_code,
        provider_instrument_code=source.row.provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_document",
            rows_written=0,
            reason="already_ingested",
            source=source,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_DOCUMENT_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info("fundamental.document_written", provider_instrument_code=source.row.provider_instrument_code, rows=written)
    return _fundamental_bronze_write(
        bronze_table="fundamental_document",
        rows_written=written,
        source=source,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-stock-identity",
    task_run_name=_instrument_task_run_name("write-bronze-fundamental-stock-identity"),
)
def write_bronze_fundamental_stock_identity(
    source: BronzeParseResult[FundamentalStockIdentitySnapshot] | None,
    source_uri: str | None = None,
    provider_instrument_code: str | None = None,
) -> BronzeWrite:
    """Write one stock identity row to ``bronze.fundamental_stock_identity``."""
    from core.lake import get_lake_client

    if source is None:
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_identity",
            rows_written=0,
            reason="not_stock",
            provider_instrument_code=provider_instrument_code,
            source_uri=source_uri,
        )

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_IDENTITY_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        provider_exchange_code=source.row.provider_exchange_code,
        provider_instrument_code=source.row.provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_identity",
            rows_written=0,
            reason="already_ingested",
            source=source,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STOCK_IDENTITY_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info(
        "fundamental.stock_identity_written", provider_instrument_code=source.row.provider_instrument_code, rows=written
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_stock_identity",
        rows_written=written,
        source=source,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-statement-facts",
    task_run_name="write-bronze-fundamental-statement-facts-{provider_instrument_code}",
)
def write_bronze_fundamental_statement_facts(
    sources: list[BronzeParseResult[FundamentalStatementFact]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write stock financial-statement facts to ``bronze.fundamental_statement_fact``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_statement_fact",
            rows_written=0,
            reason="no_facts",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_STATEMENT_FACT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_statement_fact",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STATEMENT_FACT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.statement_facts_written", provider_instrument_code=provider_instrument_code, rows=written)
    return _fundamental_bronze_write(
        bronze_table="fundamental_statement_fact",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-stock-earnings-facts",
    task_run_name="write-bronze-fundamental-stock-earnings-facts-{provider_instrument_code}",
)
def write_bronze_fundamental_stock_earnings_facts(
    sources: list[BronzeParseResult[FundamentalStockEarningsFact]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write stock earnings facts to ``bronze.fundamental_stock_earnings_fact``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_earnings_fact",
            rows_written=0,
            reason="no_earnings_facts",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_EARNINGS_FACT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_earnings_fact",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STOCK_EARNINGS_FACT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info(
        "fundamental.stock_earnings_facts_written", provider_instrument_code=provider_instrument_code, rows=written
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_stock_earnings_fact",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-stock-shares-stats",
    task_run_name=_instrument_task_run_name("write-bronze-fundamental-stock-shares-stats"),
)
def write_bronze_fundamental_stock_shares_stats(
    source: BronzeParseResult[FundamentalStockSharesStatsSnapshot] | None,
    source_uri: str | None = None,
    provider_instrument_code: str | None = None,
) -> BronzeWrite:
    """Write one stock shares-statistics row to ``bronze.fundamental_stock_shares_stats``."""
    from core.lake import get_lake_client

    if source is None:
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_shares_stats",
            rows_written=0,
            reason="no_shares_stats",
            provider_instrument_code=provider_instrument_code,
            source_uri=source_uri,
        )

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_SHARES_STATS_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        provider_exchange_code=source.row.provider_exchange_code,
        provider_instrument_code=source.row.provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_shares_stats",
            rows_written=0,
            reason="already_ingested",
            source=source,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STOCK_SHARES_STATS_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info(
        "fundamental.stock_shares_stats_written",
        provider_instrument_code=source.row.provider_instrument_code,
        rows=written,
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_stock_shares_stats",
        rows_written=written,
        source=source,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-stock-outstanding-shares",
    task_run_name="write-bronze-fundamental-stock-outstanding-shares-{provider_instrument_code}",
)
def write_bronze_fundamental_stock_outstanding_shares(
    sources: list[BronzeParseResult[FundamentalStockOutstandingShares]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write stock outstanding-shares history to ``bronze.fundamental_stock_outstanding_shares``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_outstanding_shares",
            rows_written=0,
            reason="no_outstanding_shares",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_outstanding_shares",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info(
        "fundamental.stock_outstanding_shares_written", provider_instrument_code=provider_instrument_code, rows=written
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_stock_outstanding_shares",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-stock-holders",
    task_run_name="write-bronze-fundamental-stock-holders-{provider_instrument_code}",
)
def write_bronze_fundamental_stock_holders(
    sources: list[BronzeParseResult[FundamentalStockHolder]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write stock holder rows to ``bronze.fundamental_stock_holder``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_holder",
            rows_written=0,
            reason="no_holders",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_HOLDER_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_holder",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STOCK_HOLDER_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.stock_holders_written", provider_instrument_code=provider_instrument_code, rows=written)
    return _fundamental_bronze_write(
        bronze_table="fundamental_stock_holder",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-stock-insider-transactions",
    task_run_name="write-bronze-fundamental-stock-insider-transactions-{provider_instrument_code}",
)
def write_bronze_fundamental_stock_insider_transactions(
    sources: list[BronzeParseResult[FundamentalStockInsiderTransaction]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write stock insider transaction rows to ``bronze.fundamental_stock_insider_transaction``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_insider_transaction",
            rows_written=0,
            reason="no_insider_transactions",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_INSIDER_TRANSACTION_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_insider_transaction",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STOCK_INSIDER_TRANSACTION_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info(
        "fundamental.stock_insider_transactions_written",
        provider_instrument_code=provider_instrument_code,
        rows=written,
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_stock_insider_transaction",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-stock-splits-dividends",
    task_run_name=_instrument_task_run_name("write-bronze-fundamental-stock-splits-dividends"),
)
def write_bronze_fundamental_stock_splits_dividends(
    source: BronzeParseResult[FundamentalStockSplitsDividendsSnapshot] | None,
    source_uri: str | None = None,
    provider_instrument_code: str | None = None,
) -> BronzeWrite:
    """Write one stock splits/dividends row to ``bronze.fundamental_stock_splits_dividends``."""
    from core.lake import get_lake_client

    if source is None:
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_splits_dividends",
            rows_written=0,
            reason="no_splits_dividends",
            provider_instrument_code=provider_instrument_code,
            source_uri=source_uri,
        )

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_SPLITS_DIVIDENDS_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        provider_exchange_code=source.row.provider_exchange_code,
        provider_instrument_code=source.row.provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_splits_dividends",
            rows_written=0,
            reason="already_ingested",
            source=source,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STOCK_SPLITS_DIVIDENDS_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info(
        "fundamental.stock_splits_dividends_written",
        provider_instrument_code=source.row.provider_instrument_code,
        rows=written,
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_stock_splits_dividends",
        rows_written=written,
        source=source,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-stock-dividend-counts",
    task_run_name="write-bronze-fundamental-stock-dividend-counts-{provider_instrument_code}",
)
def write_bronze_fundamental_stock_dividend_counts(
    sources: list[BronzeParseResult[FundamentalStockDividendCount]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write yearly dividend-count rows to ``bronze.fundamental_stock_dividend_count``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_dividend_count",
            rows_written=0,
            reason="no_dividend_counts",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_DIVIDEND_COUNT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_dividend_count",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STOCK_DIVIDEND_COUNT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info(
        "fundamental.stock_dividend_counts_written", provider_instrument_code=provider_instrument_code, rows=written
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_stock_dividend_count",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-stock-metric-facts",
    task_run_name="write-bronze-fundamental-stock-metric-facts-{provider_instrument_code}",
)
def write_bronze_fundamental_stock_metric_facts(
    sources: list[BronzeParseResult[FundamentalStockMetricFact]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write compact stock numeric metrics to ``bronze.fundamental_stock_metric_fact``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_metric_fact",
            rows_written=0,
            reason="no_metric_facts",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_METRIC_FACT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_metric_fact",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STOCK_METRIC_FACT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.stock_metric_facts_written", provider_instrument_code=provider_instrument_code, rows=written)
    return _fundamental_bronze_write(
        bronze_table="fundamental_stock_metric_fact",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-stock-esg-activities",
    task_run_name="write-bronze-fundamental-stock-esg-activities-{provider_instrument_code}",
)
def write_bronze_fundamental_stock_esg_activities(
    sources: list[BronzeParseResult[FundamentalStockEsgActivity]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write ESG activity involvement rows to ``bronze.fundamental_stock_esg_activity``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_esg_activity",
            rows_written=0,
            reason="no_esg_activities",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_ESG_ACTIVITY_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_stock_esg_activity",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_STOCK_ESG_ACTIVITY_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info(
        "fundamental.stock_esg_activities_written", provider_instrument_code=provider_instrument_code, rows=written
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_stock_esg_activity",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-etf-identity",
    task_run_name=_instrument_task_run_name("write-bronze-fundamental-etf-identity"),
)
def write_bronze_fundamental_etf_identity(
    source: BronzeParseResult[FundamentalEtfIdentitySnapshot] | None,
    source_uri: str | None = None,
    provider_instrument_code: str | None = None,
) -> BronzeWrite:
    """Write one ETF identity row to ``bronze.fundamental_etf_identity``."""
    from core.lake import get_lake_client

    if source is None:
        return _fundamental_bronze_write(
            bronze_table="fundamental_etf_identity",
            rows_written=0,
            reason="not_etf",
            provider_instrument_code=provider_instrument_code,
            source_uri=source_uri,
        )

    lake = get_lake_client()
    if FUNDAMENTAL_ETF_IDENTITY_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        provider_exchange_code=source.row.provider_exchange_code,
        provider_instrument_code=source.row.provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_etf_identity",
            rows_written=0,
            reason="already_ingested",
            source=source,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_ETF_IDENTITY_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info(
        "fundamental.etf_identity_written", provider_instrument_code=source.row.provider_instrument_code, rows=written
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_etf_identity",
        rows_written=written,
        source=source,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-mutual-fund-identity",
    task_run_name=_instrument_task_run_name("write-bronze-fundamental-mutual-fund-identity"),
)
def write_bronze_fundamental_mutual_fund_identity(
    source: BronzeParseResult[FundamentalMutualFundIdentitySnapshot] | None,
    source_uri: str | None = None,
    provider_instrument_code: str | None = None,
) -> BronzeWrite:
    """Write one mutual fund identity row to ``bronze.fundamental_mutual_fund_identity``."""
    from core.lake import get_lake_client

    if source is None:
        return _fundamental_bronze_write(
            bronze_table="fundamental_mutual_fund_identity",
            rows_written=0,
            reason="not_mutual_fund",
            provider_instrument_code=provider_instrument_code,
            source_uri=source_uri,
        )

    lake = get_lake_client()
    if FUNDAMENTAL_MUTUAL_FUND_IDENTITY_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        provider_exchange_code=source.row.provider_exchange_code,
        provider_instrument_code=source.row.provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_mutual_fund_identity",
            rows_written=0,
            reason="already_ingested",
            source=source,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_MUTUAL_FUND_IDENTITY_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info(
        "fundamental.mutual_fund_identity_written",
        provider_instrument_code=source.row.provider_instrument_code,
        rows=written,
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_mutual_fund_identity",
        rows_written=written,
        source=source,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-index-identity",
    task_run_name=_instrument_task_run_name("write-bronze-fundamental-index-identity"),
)
def write_bronze_fundamental_index_identity(
    source: BronzeParseResult[FundamentalIndexIdentitySnapshot] | None,
    source_uri: str | None = None,
    provider_instrument_code: str | None = None,
) -> BronzeWrite:
    """Write one index identity row to ``bronze.fundamental_index_identity``."""
    from core.lake import get_lake_client

    if source is None:
        return _fundamental_bronze_write(
            bronze_table="fundamental_index_identity",
            rows_written=0,
            reason="not_index",
            provider_instrument_code=provider_instrument_code,
            source_uri=source_uri,
        )

    lake = get_lake_client()
    if FUNDAMENTAL_INDEX_IDENTITY_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        provider_exchange_code=source.row.provider_exchange_code,
        provider_instrument_code=source.row.provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_index_identity",
            rows_written=0,
            reason="already_ingested",
            source=source,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_INDEX_IDENTITY_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info(
        "fundamental.index_identity_written", provider_instrument_code=source.row.provider_instrument_code, rows=written
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_index_identity",
        rows_written=written,
        source=source,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-etf-holdings",
    task_run_name="write-bronze-fundamental-etf-holdings-{provider_instrument_code}",
)
def write_bronze_fundamental_etf_holdings(
    sources: list[BronzeParseResult[FundamentalEtfHolding]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write ETF holdings to ``bronze.fundamental_etf_holding``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_etf_holding",
            rows_written=0,
            reason="no_etf_holdings",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_ETF_HOLDING_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_etf_holding",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_ETF_HOLDING_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.etf_holdings_written", provider_instrument_code=provider_instrument_code, rows=written)
    return _fundamental_bronze_write(
        bronze_table="fundamental_etf_holding",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-mutual-fund-holdings",
    task_run_name="write-bronze-fundamental-mutual-fund-holdings-{provider_instrument_code}",
)
def write_bronze_fundamental_mutual_fund_holdings(
    sources: list[BronzeParseResult[FundamentalMutualFundHolding]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write mutual fund holdings to ``bronze.fundamental_mutual_fund_holding``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_mutual_fund_holding",
            rows_written=0,
            reason="no_mutual_fund_holdings",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_MUTUAL_FUND_HOLDING_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_mutual_fund_holding",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_MUTUAL_FUND_HOLDING_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info(
        "fundamental.mutual_fund_holdings_written", provider_instrument_code=provider_instrument_code, rows=written
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_mutual_fund_holding",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-fund-metric-facts",
    task_run_name="write-bronze-fundamental-fund-metric-facts-{provider_instrument_code}",
)
def write_bronze_fundamental_fund_metric_facts(
    sources: list[BronzeParseResult[FundamentalFundMetricFact]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write ETF/fund metric facts to ``bronze.fundamental_fund_metric_fact``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_fund_metric_fact",
            rows_written=0,
            reason="no_fund_metric_facts",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_FUND_METRIC_FACT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_fund_metric_fact",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_FUND_METRIC_FACT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.fund_metric_facts_written", provider_instrument_code=provider_instrument_code, rows=written)
    return _fundamental_bronze_write(
        bronze_table="fundamental_fund_metric_fact",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-index-components",
    task_run_name="write-bronze-fundamental-index-components-{provider_instrument_code}",
)
def write_bronze_fundamental_index_components(
    sources: list[BronzeParseResult[FundamentalIndexComponent]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write current index components to ``bronze.fundamental_index_component``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_index_component",
            rows_written=0,
            reason="no_index_components",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_INDEX_COMPONENT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_index_component",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_INDEX_COMPONENT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.index_components_written", provider_instrument_code=provider_instrument_code, rows=written)
    return _fundamental_bronze_write(
        bronze_table="fundamental_index_component",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )


@task(
    name="write-bronze-fundamental-index-historical-components",
    task_run_name="write-bronze-fundamental-index-historical-components-{provider_instrument_code}",
)
def write_bronze_fundamental_index_historical_components(
    sources: list[BronzeParseResult[FundamentalIndexHistoricalComponent]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write historical index components to ``bronze.fundamental_index_historical_component``."""
    from core.lake import get_lake_client

    if not sources:
        return _fundamental_bronze_write(
            bronze_table="fundamental_index_historical_component",
            rows_written=0,
            reason="no_index_historical_components",
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        )

    provider_exchange_code = sources[0].row.provider_exchange_code
    lake = get_lake_client()
    if FUNDAMENTAL_INDEX_HISTORICAL_COMPONENT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
    ):
        return _fundamental_bronze_write(
            bronze_table="fundamental_index_historical_component",
            rows_written=0,
            reason="already_ingested",
            sources=sources,
            source_uri=source_uri,
        )
    written = FUNDAMENTAL_INDEX_HISTORICAL_COMPONENT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info(
        "fundamental.index_historical_components_written",
        provider_instrument_code=provider_instrument_code,
        rows=written,
    )
    return _fundamental_bronze_write(
        bronze_table="fundamental_index_historical_component",
        rows_written=written,
        sources=sources,
        source_uri=source_uri,
    )
