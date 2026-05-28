"""Bronze dataset specs for EOD price ingestion."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import BronzeDataset, LandingDomain, LandingTarget
from core.ingestion.landing import PartitionedLandingTarget
from core.ingestion.partitioning import LandingPartitionSchema
from domains.eod_price.tables import EOD_PRICE_TABLE
from providers.registry import Provider


class EODPriceDailyPartition(LandingPartitionSchema):
    """Daily price landing partitions.

    Attributes:
        provider_exchange_code: Provider exchange code requested from EODHD.
        bar_date: Trading date represented by the payload.
    """

    provider_exchange_code: str
    bar_date: date


class EODPriceBackfillPartition(LandingPartitionSchema):
    """Historical backfill landing partitions.

    Attributes:
        provider_exchange_code: Provider exchange code requested from EODHD.
        ticker: Qualified ticker being backfilled.
        from_date: Inclusive backfill start date.
        to_date: Inclusive backfill end date.
    """

    provider_exchange_code: str
    ticker: str
    from_date: date
    to_date: date


EOD_PRICE_DAILY_LANDING = LandingTarget.partitioned(
    domain=LandingDomain.EOD_PRICE,
    partition_fields=EODPriceDailyPartition,
    file_format="jsonl",
)

EOD_PRICE_BACKFILL_LANDING = LandingTarget.partitioned(
    domain=LandingDomain.EOD_PRICE,
    partition_fields=EODPriceBackfillPartition,
    file_format="jsonl",
)


@dataclass(frozen=True, slots=True)
class EODPriceLandings:
    """Landing targets that can produce ``bronze.eod_price`` rows.

    Attributes:
        daily: Bulk exchange/date landing target.
        backfill: Historical per-ticker backfill landing target.
    """

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
