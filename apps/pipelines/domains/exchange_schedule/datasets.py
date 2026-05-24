"""Bronze dataset specs for exchange schedule and holiday."""

from core.clients.storage.s3 import S3Domain
from core.ingestion import BronzeDataset, LandingSpec
from domains.exchange_schedule.models import ExchangeHolidaySnapshot, ExchangeScheduleSnapshot
from domains.exchange_schedule.tables import EXCHANGE_HOLIDAY_TABLE, EXCHANGE_SCHEDULE_TABLE
from providers.registry import Provider

EXCHANGE_SCHEDULE_LANDING = LandingSpec(
    s3_domain=S3Domain.EXCHANGE_SCHEDULE,
    style="partitioned",
    partition_fields=("exchange",),
    file_format="json",
)

EXCHANGE_SCHEDULE_DATASET = BronzeDataset[ExchangeScheduleSnapshot](
    provider=Provider.EODHD,
    table=EXCHANGE_SCHEDULE_TABLE,
    landing=EXCHANGE_SCHEDULE_LANDING,
)

EXCHANGE_HOLIDAY_DATASET = BronzeDataset[ExchangeHolidaySnapshot](
    provider=Provider.EODHD,
    table=EXCHANGE_HOLIDAY_TABLE,
    landing=EXCHANGE_SCHEDULE_LANDING,
)

__all__ = [
    "EXCHANGE_HOLIDAY_DATASET",
    "EXCHANGE_SCHEDULE_DATASET",
    "EXCHANGE_SCHEDULE_LANDING",
]
