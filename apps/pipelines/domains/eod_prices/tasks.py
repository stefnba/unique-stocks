"""
Prefect tasks for EOD price ingestion.

Tasks are atomic, retryable units. Each task does exactly one thing:
fetch, validate, or write. No business logic.
"""

from datetime import date

import structlog
from prefect import task
from prefect.tasks import exponential_backoff

from core.clients.eodhd import EODHDClient
from core.config import get_settings

from .models import EODBar

from .transforms import bars_to_bronze_records, parse_eod_bars

log = structlog.get_logger(__name__)


@task(
    name="fetch-eod-prices-bulk",
    retries=3,
    retry_delay_seconds=exponential_backoff(10),
    log_prints=True,
)
async def fetch_eod_prices_bulk(exchange: str, bar_date: date) -> list[dict]:
    """
    Fetch raw EOD price rows for an entire exchange via the EODHD bulk endpoint.
    Returns raw dicts — parsing/validation happens in a separate task.
    """
    log.info("prices.fetch_start", exchange=exchange, bar_date=bar_date)
    async with EODHDClient(api_key=get_settings().eodhd_api_key) as client:
        rows = await client.get_eod_prices_bulk(exchange=exchange, bar_date=bar_date)
    log.info("prices.fetch_done", exchange=exchange, bar_date=bar_date, rows=len(rows))
    return rows


@task(name="parse-eod-prices")
def parse_eod_prices(raw_rows: list[dict], bar_date: date) -> list[EODBar]:
    """
    Validate raw rows into EODBar models. Logs and drops invalid rows.
    """
    valid, rejected = parse_eod_bars(raw_rows, expected_date=bar_date)
    if rejected:
        log.warning(
            "prices.parse_rejections",
            count=len(rejected),
            bar_date=bar_date,
        )
    log.info("prices.parsed", valid=len(valid), rejected=len(rejected), bar_date=bar_date)
    return valid


@task(name="write-bronze-eod-prices")
def write_bronze_eod_prices(bars: list[EODBar], exchange: str, bar_date: date) -> int:
    """
    Write validated bars to bronze.eod_prices. Skips already-ingested tickers
    for this exchange+date to ensure idempotency on re-run.
    """
    from core import lake

    if not bars:
        log.info("prices.write_skipped", reason="no_bars", exchange=exchange, bar_date=bar_date)
        return 0

    # Idempotency: if we already have any rows for this exchange+date, skip entirely.
    # This is a coarse check — good enough for daily bulk ingestion.
    if lake.already_ingested_exchange_date("eod_prices", exchange, bar_date):
        log.info(
            "prices.write_skipped",
            reason="already_ingested",
            exchange=exchange,
            bar_date=bar_date,
        )
        return 0

    records = bars_to_bronze_records(bars)
    written = lake.insert_rows("bronze", "eod_prices", records)
    log.info("prices.write_done", exchange=exchange, bar_date=bar_date, rows=written)
    return written
