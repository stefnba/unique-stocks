"""Prefect tasks for fundamentals ingestion."""

from datetime import UTC, date, datetime

import structlog
from prefect import task
from prefect.client.schemas.objects import State, TaskRun
from prefect.tasks import exponential_backoff
from pydantic import ValidationError

from config.blocks import BlockRegistry
from core.ingestion import BronzeParseResult, BronzeWrite, LandingWrite
from domains.eod_price.symbols import exchange_from_qualified_ticker
from domains.fundamental.datasets import (
    FUNDAMENTAL_DOCUMENT_DATASET,
    FUNDAMENTAL_STATEMENT_FACT_DATASET,
    FUNDAMENTAL_STOCK_IDENTITY_DATASET,
)
from domains.fundamental.models import (
    FundamentalDocument,
    FundamentalStatementFact,
    FundamentalStockIdentitySnapshot,
)
from domains.fundamental.parsers import (
    parse_fundamental_document,
    parse_stock_identity_snapshot,
    parse_stock_statement_facts,
)
from providers.eodhd.models import FundamentalRaw

log = structlog.get_logger(__name__)


def _is_retryable(task: object, task_run: TaskRun, state: State) -> bool:
    """Retry transient errors only — never retry schema validation failures."""
    exc = state.result(raise_on_failure=False)
    return not isinstance(exc, ValidationError)


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


@task(name="parse-fundamental-stock")
def parse_fundamental_stock(
    raw: FundamentalRaw,
    ticker: str,
    snapshot_date: date,
) -> tuple[
    BronzeParseResult[FundamentalDocument],
    BronzeParseResult[FundamentalStockIdentitySnapshot] | None,
    list[BronzeParseResult[FundamentalStatementFact]],
    list[dict[str, object]],
]:
    """Parse the stock fundamentals slice currently supported by this domain."""
    document = parse_fundamental_document(raw, ticker=ticker, snapshot_date=snapshot_date)
    identity = parse_stock_identity_snapshot(raw, ticker=ticker, snapshot_date=snapshot_date)
    facts, rejected = parse_stock_statement_facts(raw, ticker=ticker, snapshot_date=snapshot_date)
    log.info(
        "fundamental.parsed",
        ticker=ticker,
        family=document.row.instrument_family,
        stock_identity=identity is not None,
        statement_facts=len(facts),
        rejected=len(rejected),
    )
    return document, identity, facts, rejected


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
