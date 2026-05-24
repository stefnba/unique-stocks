"""Bronze dataset specs for EOD price ingestion."""

from core.clients.storage.s3 import S3Domain
from core.ingestion import BronzeDataset, LandingSpec
from domains.eod_price.models import EODBar
from domains.eod_price.tables import EOD_PRICE_TABLE
from providers.registry import Provider

EOD_PRICE_DAILY_LANDING = LandingSpec(
    s3_domain=S3Domain.EOD_PRICE,
    style="partitioned",
    partition_fields=("exchange", "bar_date"),
    file_format="jsonl",
)

EOD_PRICE_BACKFILL_LANDING = LandingSpec(
    s3_domain=S3Domain.EOD_PRICE,
    style="partitioned",
    partition_fields=("exchange", "ticker", "from_date", "to_date"),
    file_format="jsonl",
)

EOD_PRICE_DATASET = BronzeDataset[EODBar](
    provider=Provider.EODHD,
    table=EOD_PRICE_TABLE,
    landing=EOD_PRICE_DAILY_LANDING,
)

__all__ = [
    "EOD_PRICE_BACKFILL_LANDING",
    "EOD_PRICE_DAILY_LANDING",
    "EOD_PRICE_DATASET",
]
