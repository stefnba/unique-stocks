"""Bronze dataset specs for exchange schedules and holidays."""

from core.clients.storage.s3 import S3Domain
from core.ingestion import BronzeDataset, LandingSpec
from domains.exchange_schedules.models import ExchangeHolidaySnapshot, ExchangeScheduleSnapshot
from domains.exchange_schedules.tables import EXCHANGE_HOLIDAYS_TABLE, EXCHANGE_SCHEDULES_TABLE
from providers.registry import Provider

EXCHANGE_SCHEDULES_LANDING = LandingSpec(
    s3_domain=S3Domain.EXCHANGE_SCHEDULES,
    style="partitioned",
    partition_fields=("exchange",),
    file_format="json",
)

EXCHANGE_SCHEDULES_DATASET = BronzeDataset[ExchangeScheduleSnapshot](
    provider=Provider.EODHD,
    table=EXCHANGE_SCHEDULES_TABLE,
    landing=EXCHANGE_SCHEDULES_LANDING,
)

EXCHANGE_HOLIDAYS_DATASET = BronzeDataset[ExchangeHolidaySnapshot](
    provider=Provider.EODHD,
    table=EXCHANGE_HOLIDAYS_TABLE,
    landing=EXCHANGE_SCHEDULES_LANDING,
)

__all__ = [
    "EXCHANGE_HOLIDAYS_DATASET",
    "EXCHANGE_SCHEDULES_DATASET",
    "EXCHANGE_SCHEDULES_LANDING",
]
