

from prefect import task

from core.config import get_settings
from providers.eodhd.models import SupportedExchanges

@task(
    name="fetch-supported-exchanges",
    retries=3,
    log_prints=True,
)
async def fetch_supported_exchanges() -> list[SupportedExchanges]   :
    """Fetch and schema-validate raw EOD price rows for an entire exchange.

    Uses the EODHD bulk endpoint (one API call per exchange).
    Raises ValidationError if EODHD's response shape doesn't match EODBulkPriceRaw.
    """
    from providers.eodhd.client import EODHDClient

    async with EODHDClient(api_key=get_settings().eodhd_api_key) as client:
        exchanges = await client.get_exchanges()

    return exchanges



@task(name="write-bronze-exchanges")
def write_to_landing_zone(exchanges: list[SupportedExchanges]):
    """Write supported exchanges to the landing zone."""
    
    pass