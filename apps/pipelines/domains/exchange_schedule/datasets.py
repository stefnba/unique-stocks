"""Bronze dataset specs for exchange schedule and holiday."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import BronzeDataset, LandingDomain, LandingTarget
from core.ingestion.landing import PartitionedLandingTarget
from core.ingestion.partitioning import LandingPartitionSchema
from domains.exchange_schedule.tables import EXCHANGE_HOLIDAY_TABLE, EXCHANGE_SCHEDULE_TABLE
from providers.registry import Provider


class ExchangeScheduleLandingPartition(LandingPartitionSchema):
    """Exchange schedule landing partitions."""

    provider_schedule_exchange_code: str
    snapshot_date: date


EXCHANGE_SCHEDULE_LANDING = LandingTarget.partitioned(
    domain=LandingDomain.EXCHANGE_SCHEDULE,
    partition_fields=ExchangeScheduleLandingPartition,
    file_format="json",
)


@dataclass(frozen=True, slots=True)
class ExchangeScheduleLandings:
    """Landing targets that can produce exchange schedule Bronze rows."""

    details: PartitionedLandingTarget[ExchangeScheduleLandingPartition]


EXCHANGE_SCHEDULE_LANDINGS = ExchangeScheduleLandings(details=EXCHANGE_SCHEDULE_LANDING)

EXCHANGE_SCHEDULE_DATASET = BronzeDataset(
    provider=Provider.EODHD,
    table=EXCHANGE_SCHEDULE_TABLE,
    landings=EXCHANGE_SCHEDULE_LANDINGS,
)

EXCHANGE_HOLIDAY_DATASET = BronzeDataset(
    provider=Provider.EODHD,
    table=EXCHANGE_HOLIDAY_TABLE,
    landings=EXCHANGE_SCHEDULE_LANDINGS,
)

__all__ = [
    "EXCHANGE_HOLIDAY_DATASET",
    "EXCHANGE_SCHEDULE_DATASET",
    "EXCHANGE_SCHEDULE_LANDING",
    "ExchangeScheduleLandingPartition",
    "ExchangeScheduleLandings",
]
