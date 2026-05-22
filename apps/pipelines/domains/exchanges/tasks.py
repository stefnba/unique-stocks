

from prefect import task

from config.blocks import BlockRegistry
from core.clients.storage.s3 import S3Key
from providers.eodhd.models import SupportedExchange

@task(
    retries=3,
    log_prints=True,
)
async def fetch_supported_exchanges() -> list[SupportedExchange]   :
    """Fetch and schema-validate raw EOD price rows for an entire exchange.

    Uses the EODHD bulk endpoint (one API call per exchange).
    Raises ValidationError if EODHD's response shape doesn't match EODBulkPriceRaw.
    """
    from providers.eodhd.client import EODHDClient

    api_key = await BlockRegistry.EODHD_API_KEY.load_async()
    async with EODHDClient(api_key=api_key.get()) as client:
        exchanges = await client.get_exchanges()

    return exchanges



@task()
async def write_to_landing_zone(exchanges: list[SupportedExchange]) -> str:
    """Write supported exchanges to the S3 landing zone as JSONL."""
    from core.clients.storage.s3 import S3StorageClient

    s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
    key = S3Key.snapshot(S3Key.Provider.EODHD, S3Key.Domain.EXCHANGES).jsonl()
    ref = s3.save(key, exchanges)
    return ref.uri