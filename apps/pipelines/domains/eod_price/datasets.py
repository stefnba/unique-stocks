"""Bronze dataset specs for EOD price ingestion."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import BronzeDataset, LandingDomain, LandingTarget
from core.ingestion.landing import PartitionedLandingTarget
from core.ingestion.partitioning import LandingPartitionSchema
from domains.eod_price.tables import EOD_PRICE_TABLE
from providers.registry import Provider


class EODPriceDailyPartition(LandingPartitionSchema):
    """Daily price landing partitions."""

    provider_exchange_code: str
    bar_date: date


class EODPriceBackfillPartition(LandingPartitionSchema):
    """Historical backfill landing partitions."""

    provider_exchange_code: str
    ticker: str
    from_date: date
    to_date: date


EOD_PRICE_DAILY_LANDING = LandingTarget.partitioned(
    domain=LandingDomain.EOD_PRICE,
    partition_fields=EODPriceDailyPartition,
    file_format="jsonl",
    audit_dataset="eod_price.daily",
)

EOD_PRICE_BACKFILL_LANDING = LandingTarget.partitioned(
    domain=LandingDomain.EOD_PRICE,
    partition_fields=EODPriceBackfillPartition,
    file_format="jsonl",
    audit_dataset="eod_price.backfill",
)


@dataclass(frozen=True, slots=True)
class EODPriceLandings:
    """Landing targets that can produce ``bronze.eod_price`` rows."""

    daily: PartitionedLandingTarget[EODPriceDailyPartition]
    backfill: PartitionedLandingTarget[EODPriceBackfillPartition]


EOD_PRICE_DATASET = BronzeDataset(
    provider=Provider.EODHD,
    table=EOD_PRICE_TABLE,
    landings=EODPriceLandings(daily=EOD_PRICE_DAILY_LANDING, backfill=EOD_PRICE_BACKFILL_LANDING),
)

__all__ = [
    "EOD_PRICE_BACKFILL_LANDING",
    "EOD_PRICE_DAILY_LANDING",
    "EOD_PRICE_DATASET",
    "EODPriceBackfillPartition",
    "EODPriceDailyPartition",
    "EODPriceLandings",
]
