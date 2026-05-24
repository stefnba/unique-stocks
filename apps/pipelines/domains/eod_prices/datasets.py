"""Bronze dataset specs for EOD price ingestion."""

from core.clients.storage.s3 import S3Domain
from core.ingestion import BronzeDataset, LandingSpec
from domains.eod_prices.models import EODBar
from domains.eod_prices.tables import EOD_PRICES_TABLE
from providers.registry import Provider

EOD_PRICES_DAILY_LANDING = LandingSpec(
    s3_domain=S3Domain.EOD_PRICES,
    style="partitioned",
    partition_fields=("exchange", "bar_date"),
    file_format="jsonl",
)

EOD_PRICES_BACKFILL_LANDING = LandingSpec(
    s3_domain=S3Domain.EOD_PRICES,
    style="partitioned",
    partition_fields=("exchange", "ticker", "from_date", "to_date"),
    file_format="jsonl",
)

EOD_PRICES_DATASET = BronzeDataset[EODBar](
    provider=Provider.EODHD,
    table=EOD_PRICES_TABLE,
    landing=EOD_PRICES_DAILY_LANDING,
)

__all__ = [
    "EOD_PRICES_BACKFILL_LANDING",
    "EOD_PRICES_DAILY_LANDING",
    "EOD_PRICES_DATASET",
]
