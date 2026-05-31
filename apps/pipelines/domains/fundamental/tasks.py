"""Prefect tasks for fundamentals ingestion."""

from datetime import UTC, date, datetime
from typing import Protocol

import structlog
from prefect import task
from prefect.client.schemas.objects import State, TaskRun
from prefect.tasks import exponential_backoff
from pydantic import ValidationError

from config.blocks import BlockRegistry
from core.ingestion import BronzeParseResult, BronzeWrite, LandingWrite
from core.ingestion.keys import ObjectStorageKey
from domains.eod_price.symbols import exchange_from_qualified_ticker
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
    parse_index_historical_components,
    parse_index_identity_snapshot,
    parse_mutual_fund_holdings,
    parse_mutual_fund_identity_snapshot,
    parse_stock_dividend_counts,
    parse_stock_earnings_facts,
    parse_stock_esg_activities,
    parse_stock_holders,
    parse_stock_identity_snapshot,
    parse_stock_metric_facts,
    parse_stock_outstanding_shares,
    parse_stock_shares_stats_snapshot,
    parse_stock_splits_dividends_snapshot,
    parse_stock_statement_facts,
)
from providers.eodhd.models import FundamentalRaw

log = structlog.get_logger(__name__)


class _LandingStorage(Protocol):
    bucket: str | None

    def list_keys(self, prefix: str = "", *, bucket: str | None = None, max_keys: int | None = None) -> list[str]:
        """Return landing object keys under a prefix."""
        ...


def _is_retryable(task: object, task_run: TaskRun, state: State) -> bool:
    """Retry transient errors only — never retry schema validation failures."""
    exc = state.result(raise_on_failure=False)
    return not isinstance(exc, ValidationError)


@task(name="fetch-fundamental-provider-exchange-codes")
def fetch_fundamental_provider_exchange_codes() -> list[str]:
    """Load provider request codes approved for automatic fundamentals selection."""
    from domains.exchange.provider_universe import load_provider_exchange_codes

    codes = load_provider_exchange_codes("eodhd", purpose="fundamental")
    log.info("fundamental.provider_exchange_codes_loaded", count=len(codes))
    return codes


@task(name="load-fundamental-stock-tickers")
def load_fundamental_stock_tickers(
    provider_exchange_codes: list[str] | None = None,
    limit: int | None = None,
) -> list[str]:
    """Load latest stock-like instruments from ``bronze.instrument``."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    if not lake.table_exists("bronze", "instrument"):
        log.warning("fundamental.no_instrument_table")
        return []

    instrument_q = lake.qualified_name("bronze", "instrument")
    params: list[object] = []
    exchange_filter = ""
    if provider_exchange_codes:
        placeholders = ", ".join("?" for _ in provider_exchange_codes)
        exchange_filter = f"AND provider_exchange_code IN ({placeholders})"
        params.extend(provider_exchange_codes)

    limit_clause = ""
    if limit is not None:
        limit_clause = "LIMIT ?"
        params.append(max(0, int(limit)))

    rows = lake.query(
        f"""
        SELECT DISTINCT ticker, provider_exchange_code
        FROM {instrument_q}
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {instrument_q})
          AND LOWER(COALESCE(asset_type, '')) LIKE '%stock%'
          {exchange_filter}
        ORDER BY provider_exchange_code, ticker
        {limit_clause}
        """,
        params,
    )
    tickers = [f"{row['ticker']}.{row['provider_exchange_code']}" for row in rows]
    log.info("fundamental.stock_tickers_loaded", count=len(tickers))
    return tickers


@task(name="fundamental-document-already-ingested")
def fundamental_document_already_ingested(ticker: str, snapshot_date: date) -> bool:
    """Return True when the fundamentals document already exists for this ticker snapshot."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    return FUNDAMENTAL_DOCUMENT_DATASET.already_ingested(lake, snapshot_date=snapshot_date, ticker=ticker)


@task(name="load-fundamental-document-payload-hash")
def load_fundamental_document_payload_hash(ticker: str, snapshot_date: date) -> str | None:
    """Return the stored payload hash for a ticker snapshot when one exists."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    if not lake.table_exists("bronze", FUNDAMENTAL_DOCUMENT_DATASET.table_name):
        return None

    qualified = lake.qualified_name("bronze", FUNDAMENTAL_DOCUMENT_DATASET.table_name)
    row = lake.query_one(
        f"""
        SELECT payload_hash
        FROM {qualified}
        WHERE snapshot_date = ?
          AND ticker = ?
          AND data_provider = ?
        ORDER BY ingested_at DESC
        LIMIT 1
        """,
        [snapshot_date.isoformat(), ticker, str(FUNDAMENTAL_DOCUMENT_DATASET.provider)],
    )
    return str(row["payload_hash"]) if row and row.get("payload_hash") else None


@task(name="delete-fundamental-snapshot-rows")
def delete_fundamental_snapshot_rows(ticker: str, snapshot_date: date) -> int:
    """Delete existing fundamentals Bronze rows for an explicit ticker snapshot.

    Used only by refresh mode after a changed provider document has already
    landed, so the same-day snapshot can be replaced without violating Bronze
    unique constraints.
    """
    from core.clients.lake import get_lake_client

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
              AND ticker = ?
              AND data_provider = ?
            """,
            [snapshot_date.isoformat(), ticker, str(dataset.provider)],
        )
        deleted_tables += 1
    log.info("fundamental.snapshot_deleted", ticker=ticker, snapshot_date=snapshot_date, tables=deleted_tables)
    return deleted_tables


@task(
    name="fetch-fundamental-ticker",
    retries=3,
    retry_delay_seconds=exponential_backoff(10),
    retry_condition_fn=_is_retryable,
)
async def fetch_fundamental_ticker(ticker: str) -> FundamentalRaw:
    """Fetch and top-level schema-validate one fundamentals document."""
    from providers.eodhd.client import EODHDClient

    log.info("fundamental.fetch_start", ticker=ticker)
    api_key = (await BlockRegistry.EODHD_API_KEY.load_async()).get()
    async with EODHDClient(api_key=api_key) as client:
        raw = await client.get_fundamental(ticker)
    log.info("fundamental.fetch_done", ticker=ticker, sections=len(raw.model_dump(exclude_none=True)))
    return raw


@task(name="load-fundamental-from-landing")
async def load_fundamental_from_landing(
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> tuple[FundamentalRaw, LandingWrite]:
    """Load one landed fundamentals document without calling the provider.

    If ``source_uri`` is omitted, the latest landing object for the ticker and
    snapshot date is selected by its path-safe ``ingested_at`` partition.
    """
    from core.clients.storage.s3 import S3StorageClient

    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    resolved_uri = source_uri or _latest_fundamental_landing_uri(s3, ticker=ticker, snapshot_date=snapshot_date)
    payload = s3.load(resolved_uri, format="json")
    raw = FundamentalRaw.model_validate(payload)
    landing = LandingWrite(
        dataset=FUNDAMENTAL_DOCUMENT_DATASET.landings.document.audit_dataset_name,
        source_uri=resolved_uri,
        partition={
            "provider_exchange_code": exchange_from_qualified_ticker(ticker),
            "ticker": ticker,
            "snapshot_date": snapshot_date,
        },
        rows_raw=1,
    )
    log.info("fundamental.landing_loaded", ticker=ticker, snapshot_date=snapshot_date, uri=resolved_uri)
    return raw, landing


@task(name="write-fundamental-landing")
async def write_fundamental_to_landing(
    raw: FundamentalRaw,
    ticker: str,
    snapshot_date: date,
    ingested_at: datetime | None = None,
) -> LandingWrite:
    """Write one raw fundamentals document to the S3 landing zone as JSON."""
    from core.clients.storage.s3 import S3StorageClient

    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    ref = FUNDAMENTAL_DOCUMENT_DATASET.landings.document.save(
        s3,
        provider=FUNDAMENTAL_DOCUMENT_DATASET.provider,
        data=raw,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "ticker": ticker,
            "snapshot_date": snapshot_date,
        },
        ingested_at=stamp,
    )
    log.info("fundamental.landing_written", ticker=ticker, snapshot_date=snapshot_date, uri=ref.uri)
    return FUNDAMENTAL_DOCUMENT_DATASET.landings.document.landing_write(
        ref,
        partitions={
            "provider_exchange_code": provider_exchange_code,
            "ticker": ticker,
            "snapshot_date": snapshot_date,
        },
        rows_raw=1,
    )


def _latest_fundamental_landing_uri(s3: _LandingStorage, *, ticker: str, snapshot_date: date) -> str:
    provider_exchange_code = exchange_from_qualified_ticker(ticker)
    key = ObjectStorageKey.partitioned_from_mapping(
        FUNDAMENTAL_DOCUMENT_DATASET.provider,
        FUNDAMENTAL_DOCUMENT_DATASET.landings.document.domain,
        {
            "provider_exchange_code": provider_exchange_code,
            "ticker": ticker,
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
            f"No landed fundamentals document found for ticker={ticker!r}, snapshot_date={snapshot_date.isoformat()!r}"
        )
    latest_key = sorted(keys)[-1]
    bucket = _landing_bucket(s3)
    return f"s3://{bucket}/{latest_key}"


def _landing_bucket(s3: _LandingStorage) -> str:
    bucket = getattr(s3, "bucket", None)
    if not bucket:
        raise ValueError("S3 storage client has no default bucket; pass an explicit fundamentals source_uri.")
    return str(bucket)


@task(name="parse-fundamental-stock")
def parse_fundamental_stock(
    raw: FundamentalRaw,
    ticker: str,
    snapshot_date: date,
) -> tuple[
    BronzeParseResult[FundamentalDocument],
    BronzeParseResult[FundamentalStockIdentitySnapshot] | None,
    list[BronzeParseResult[FundamentalStatementFact]],
    list[BronzeParseResult[FundamentalStockEarningsFact]],
    BronzeParseResult[FundamentalStockSharesStatsSnapshot] | None,
    list[BronzeParseResult[FundamentalStockOutstandingShares]],
    list[BronzeParseResult[FundamentalStockHolder]],
    BronzeParseResult[FundamentalStockSplitsDividendsSnapshot] | None,
    list[BronzeParseResult[FundamentalStockDividendCount]],
    list[BronzeParseResult[FundamentalStockMetricFact]],
    list[BronzeParseResult[FundamentalStockEsgActivity]],
    BronzeParseResult[FundamentalEtfIdentitySnapshot] | None,
    BronzeParseResult[FundamentalMutualFundIdentitySnapshot] | None,
    BronzeParseResult[FundamentalIndexIdentitySnapshot] | None,
    list[BronzeParseResult[FundamentalEtfHolding]],
    list[BronzeParseResult[FundamentalMutualFundHolding]],
    list[BronzeParseResult[FundamentalFundMetricFact]],
    list[BronzeParseResult[FundamentalIndexComponent]],
    list[BronzeParseResult[FundamentalIndexHistoricalComponent]],
    list[dict[str, object]],
]:
    """Parse the stock fundamentals slice currently supported by this domain."""
    document = parse_fundamental_document(raw, ticker=ticker, snapshot_date=snapshot_date)
    identity = parse_stock_identity_snapshot(raw, ticker=ticker, snapshot_date=snapshot_date)
    statement_facts, statement_rejected = parse_stock_statement_facts(raw, ticker=ticker, snapshot_date=snapshot_date)
    earnings_facts, earnings_rejected = parse_stock_earnings_facts(raw, ticker=ticker, snapshot_date=snapshot_date)
    shares_stats = parse_stock_shares_stats_snapshot(raw, ticker=ticker, snapshot_date=snapshot_date)
    outstanding_shares, outstanding_rejected = parse_stock_outstanding_shares(
        raw,
        ticker=ticker,
        snapshot_date=snapshot_date,
    )
    holders, holders_rejected = parse_stock_holders(raw, ticker=ticker, snapshot_date=snapshot_date)
    splits_dividends = parse_stock_splits_dividends_snapshot(raw, ticker=ticker, snapshot_date=snapshot_date)
    dividend_counts, dividend_rejected = parse_stock_dividend_counts(raw, ticker=ticker, snapshot_date=snapshot_date)
    metric_facts = parse_stock_metric_facts(raw, ticker=ticker, snapshot_date=snapshot_date)
    esg_activities, esg_rejected = parse_stock_esg_activities(raw, ticker=ticker, snapshot_date=snapshot_date)
    etf_identity = parse_etf_identity_snapshot(raw, ticker=ticker, snapshot_date=snapshot_date)
    mutual_fund_identity = parse_mutual_fund_identity_snapshot(raw, ticker=ticker, snapshot_date=snapshot_date)
    index_identity = parse_index_identity_snapshot(raw, ticker=ticker, snapshot_date=snapshot_date)
    etf_holdings, etf_rejected = parse_etf_holdings(raw, ticker=ticker, snapshot_date=snapshot_date)
    mutual_fund_holdings, mutual_fund_rejected = parse_mutual_fund_holdings(
        raw,
        ticker=ticker,
        snapshot_date=snapshot_date,
    )
    fund_metric_facts = parse_fund_metric_facts(raw, ticker=ticker, snapshot_date=snapshot_date)
    index_components, index_rejected = parse_index_components(raw, ticker=ticker, snapshot_date=snapshot_date)
    index_historical_components, index_historical_rejected = parse_index_historical_components(
        raw,
        ticker=ticker,
        snapshot_date=snapshot_date,
    )
    rejected: list[dict[str, object]] = [
        *statement_rejected,
        *earnings_rejected,
        *outstanding_rejected,
        *holders_rejected,
        *dividend_rejected,
        *esg_rejected,
        *etf_rejected,
        *mutual_fund_rejected,
        *index_rejected,
        *index_historical_rejected,
    ]
    log.info(
        "fundamental.parsed",
        ticker=ticker,
        family=document.row.instrument_family,
        stock_identity=identity is not None,
        statement_facts=len(statement_facts),
        earnings_facts=len(earnings_facts),
        shares_stats=shares_stats is not None,
        outstanding_shares=len(outstanding_shares),
        holders=len(holders),
        splits_dividends=splits_dividends is not None,
        dividend_counts=len(dividend_counts),
        metric_facts=len(metric_facts),
        esg_activities=len(esg_activities),
        etf_identity=etf_identity is not None,
        mutual_fund_identity=mutual_fund_identity is not None,
        index_identity=index_identity is not None,
        etf_holdings=len(etf_holdings),
        mutual_fund_holdings=len(mutual_fund_holdings),
        fund_metric_facts=len(fund_metric_facts),
        index_components=len(index_components),
        index_historical_components=len(index_historical_components),
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
        splits_dividends,
        dividend_counts,
        metric_facts,
        esg_activities,
        etf_identity,
        mutual_fund_identity,
        index_identity,
        etf_holdings,
        mutual_fund_holdings,
        fund_metric_facts,
        index_components,
        index_historical_components,
        rejected,
    )


@task(name="write-bronze-fundamental-document")
def write_bronze_fundamental_document(
    source: BronzeParseResult[FundamentalDocument],
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write one document metadata row to ``bronze.fundamental_document``."""
    from core.clients.lake import get_lake_client

    lake = get_lake_client()
    if FUNDAMENTAL_DOCUMENT_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        ticker=source.row.ticker,
    ):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_DOCUMENT_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info("fundamental.document_written", ticker=source.row.ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-stock-identity")
def write_bronze_fundamental_stock_identity(
    source: BronzeParseResult[FundamentalStockIdentitySnapshot] | None,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write one stock identity row to ``bronze.fundamental_stock_identity``."""
    from core.clients.lake import get_lake_client

    if source is None:
        return BronzeWrite(rows_written=0, reason="not_stock")

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_IDENTITY_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        ticker=source.row.ticker,
    ):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_STOCK_IDENTITY_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info("fundamental.stock_identity_written", ticker=source.row.ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-statement-facts")
def write_bronze_fundamental_statement_facts(
    sources: list[BronzeParseResult[FundamentalStatementFact]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write stock financial-statement facts to ``bronze.fundamental_statement_fact``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_facts")

    lake = get_lake_client()
    if FUNDAMENTAL_STATEMENT_FACT_DATASET.already_ingested(lake, snapshot_date=snapshot_date, ticker=ticker):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_STATEMENT_FACT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.statement_facts_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-stock-earnings-facts")
def write_bronze_fundamental_stock_earnings_facts(
    sources: list[BronzeParseResult[FundamentalStockEarningsFact]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write stock earnings facts to ``bronze.fundamental_stock_earnings_fact``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_earnings_facts")

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_EARNINGS_FACT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        ticker=ticker,
    ):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_STOCK_EARNINGS_FACT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.stock_earnings_facts_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-stock-shares-stats")
def write_bronze_fundamental_stock_shares_stats(
    source: BronzeParseResult[FundamentalStockSharesStatsSnapshot] | None,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write one stock shares-statistics row to ``bronze.fundamental_stock_shares_stats``."""
    from core.clients.lake import get_lake_client

    if source is None:
        return BronzeWrite(rows_written=0, reason="no_shares_stats")

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_SHARES_STATS_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        ticker=source.row.ticker,
    ):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_STOCK_SHARES_STATS_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info("fundamental.stock_shares_stats_written", ticker=source.row.ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-stock-outstanding-shares")
def write_bronze_fundamental_stock_outstanding_shares(
    sources: list[BronzeParseResult[FundamentalStockOutstandingShares]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write stock outstanding-shares history to ``bronze.fundamental_stock_outstanding_shares``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_outstanding_shares")

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        ticker=ticker,
    ):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.stock_outstanding_shares_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-stock-holders")
def write_bronze_fundamental_stock_holders(
    sources: list[BronzeParseResult[FundamentalStockHolder]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write stock holder rows to ``bronze.fundamental_stock_holder``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_holders")

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_HOLDER_DATASET.already_ingested(lake, snapshot_date=snapshot_date, ticker=ticker):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_STOCK_HOLDER_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.stock_holders_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-stock-splits-dividends")
def write_bronze_fundamental_stock_splits_dividends(
    source: BronzeParseResult[FundamentalStockSplitsDividendsSnapshot] | None,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write one stock splits/dividends row to ``bronze.fundamental_stock_splits_dividends``."""
    from core.clients.lake import get_lake_client

    if source is None:
        return BronzeWrite(rows_written=0, reason="no_splits_dividends")

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_SPLITS_DIVIDENDS_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        ticker=source.row.ticker,
    ):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_STOCK_SPLITS_DIVIDENDS_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info("fundamental.stock_splits_dividends_written", ticker=source.row.ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-stock-dividend-counts")
def write_bronze_fundamental_stock_dividend_counts(
    sources: list[BronzeParseResult[FundamentalStockDividendCount]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write yearly dividend-count rows to ``bronze.fundamental_stock_dividend_count``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_dividend_counts")

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_DIVIDEND_COUNT_DATASET.already_ingested(lake, snapshot_date=snapshot_date, ticker=ticker):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_STOCK_DIVIDEND_COUNT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.stock_dividend_counts_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-stock-metric-facts")
def write_bronze_fundamental_stock_metric_facts(
    sources: list[BronzeParseResult[FundamentalStockMetricFact]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write compact stock numeric metrics to ``bronze.fundamental_stock_metric_fact``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_metric_facts")

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_METRIC_FACT_DATASET.already_ingested(lake, snapshot_date=snapshot_date, ticker=ticker):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_STOCK_METRIC_FACT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.stock_metric_facts_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-stock-esg-activities")
def write_bronze_fundamental_stock_esg_activities(
    sources: list[BronzeParseResult[FundamentalStockEsgActivity]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write ESG activity involvement rows to ``bronze.fundamental_stock_esg_activity``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_esg_activities")

    lake = get_lake_client()
    if FUNDAMENTAL_STOCK_ESG_ACTIVITY_DATASET.already_ingested(lake, snapshot_date=snapshot_date, ticker=ticker):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_STOCK_ESG_ACTIVITY_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.stock_esg_activities_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-etf-identity")
def write_bronze_fundamental_etf_identity(
    source: BronzeParseResult[FundamentalEtfIdentitySnapshot] | None,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write one ETF identity row to ``bronze.fundamental_etf_identity``."""
    from core.clients.lake import get_lake_client

    if source is None:
        return BronzeWrite(rows_written=0, reason="not_etf")

    lake = get_lake_client()
    if FUNDAMENTAL_ETF_IDENTITY_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        ticker=source.row.ticker,
    ):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_ETF_IDENTITY_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info("fundamental.etf_identity_written", ticker=source.row.ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-mutual-fund-identity")
def write_bronze_fundamental_mutual_fund_identity(
    source: BronzeParseResult[FundamentalMutualFundIdentitySnapshot] | None,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write one mutual fund identity row to ``bronze.fundamental_mutual_fund_identity``."""
    from core.clients.lake import get_lake_client

    if source is None:
        return BronzeWrite(rows_written=0, reason="not_mutual_fund")

    lake = get_lake_client()
    if FUNDAMENTAL_MUTUAL_FUND_IDENTITY_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        ticker=source.row.ticker,
    ):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_MUTUAL_FUND_IDENTITY_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info("fundamental.mutual_fund_identity_written", ticker=source.row.ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-index-identity")
def write_bronze_fundamental_index_identity(
    source: BronzeParseResult[FundamentalIndexIdentitySnapshot] | None,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write one index identity row to ``bronze.fundamental_index_identity``."""
    from core.clients.lake import get_lake_client

    if source is None:
        return BronzeWrite(rows_written=0, reason="not_index")

    lake = get_lake_client()
    if FUNDAMENTAL_INDEX_IDENTITY_DATASET.already_ingested(
        lake,
        snapshot_date=source.row.snapshot_date,
        ticker=source.row.ticker,
    ):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_INDEX_IDENTITY_DATASET.write_bronze(lake, [source], source_uri=source_uri)
    log.info("fundamental.index_identity_written", ticker=source.row.ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-etf-holdings")
def write_bronze_fundamental_etf_holdings(
    sources: list[BronzeParseResult[FundamentalEtfHolding]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write ETF holdings to ``bronze.fundamental_etf_holding``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_etf_holdings")

    lake = get_lake_client()
    if FUNDAMENTAL_ETF_HOLDING_DATASET.already_ingested(lake, snapshot_date=snapshot_date, ticker=ticker):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_ETF_HOLDING_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.etf_holdings_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-mutual-fund-holdings")
def write_bronze_fundamental_mutual_fund_holdings(
    sources: list[BronzeParseResult[FundamentalMutualFundHolding]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write mutual fund holdings to ``bronze.fundamental_mutual_fund_holding``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_mutual_fund_holdings")

    lake = get_lake_client()
    if FUNDAMENTAL_MUTUAL_FUND_HOLDING_DATASET.already_ingested(lake, snapshot_date=snapshot_date, ticker=ticker):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_MUTUAL_FUND_HOLDING_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.mutual_fund_holdings_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-fund-metric-facts")
def write_bronze_fundamental_fund_metric_facts(
    sources: list[BronzeParseResult[FundamentalFundMetricFact]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write ETF/fund metric facts to ``bronze.fundamental_fund_metric_fact``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_fund_metric_facts")

    lake = get_lake_client()
    if FUNDAMENTAL_FUND_METRIC_FACT_DATASET.already_ingested(lake, snapshot_date=snapshot_date, ticker=ticker):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_FUND_METRIC_FACT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.fund_metric_facts_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-index-components")
def write_bronze_fundamental_index_components(
    sources: list[BronzeParseResult[FundamentalIndexComponent]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write current index components to ``bronze.fundamental_index_component``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_index_components")

    lake = get_lake_client()
    if FUNDAMENTAL_INDEX_COMPONENT_DATASET.already_ingested(lake, snapshot_date=snapshot_date, ticker=ticker):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_INDEX_COMPONENT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.index_components_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)


@task(name="write-bronze-fundamental-index-historical-components")
def write_bronze_fundamental_index_historical_components(
    sources: list[BronzeParseResult[FundamentalIndexHistoricalComponent]],
    ticker: str,
    snapshot_date: date,
    source_uri: str | None = None,
) -> BronzeWrite:
    """Write historical index components to ``bronze.fundamental_index_historical_component``."""
    from core.clients.lake import get_lake_client

    if not sources:
        return BronzeWrite(rows_written=0, reason="no_index_historical_components")

    lake = get_lake_client()
    if FUNDAMENTAL_INDEX_HISTORICAL_COMPONENT_DATASET.already_ingested(
        lake,
        snapshot_date=snapshot_date,
        ticker=ticker,
    ):
        return BronzeWrite(rows_written=0, reason="already_ingested")
    written = FUNDAMENTAL_INDEX_HISTORICAL_COMPONENT_DATASET.write_bronze(lake, sources, source_uri=source_uri)
    log.info("fundamental.index_historical_components_written", ticker=ticker, rows=written)
    return BronzeWrite(rows_written=written)
