"""Bronze dataset specs for EOD price ingestion."""

from dataclasses import dataclass
from datetime import date

from config.domains import Domain
from config.providers import Provider
from core.ingestion import BronzeDataset, LandingTarget
from core.ingestion.landing import PartitionedLandingTarget
from core.ingestion.partitioning import LandingPartitionSchema
from domains.eod_price.tables import EOD_PRICE_TABLE


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
        provider_instrument_code: Provider instrument code being backfilled.
        from_date: Inclusive backfill start date, or ``"all"`` when the
            provider start parameter is omitted.
        to_date: Inclusive backfill end date.
    """

    provider_exchange_code: str
    provider_instrument_code: str
    from_date: date | str
    to_date: date


EOD_PRICE_DAILY_LANDING = LandingTarget.partitioned(
    domain=Domain.EOD_PRICE,
    partition_fields=EODPriceDailyPartition,
    file_format="jsonl",
)

EOD_PRICE_BACKFILL_LANDING = LandingTarget.partitioned(
    domain=Domain.EOD_PRICE,
    partition_fields=EODPriceBackfillPartition,
    file_format="jsonl",
)


@dataclass(frozen=True, slots=True)
class EODPriceLandings:
    """Landing targets that can produce ``bronze.eod_price`` rows.

    Attributes:
        daily: Bulk exchange/date landing target.
        backfill: Historical per-instrument backfill landing target.
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
