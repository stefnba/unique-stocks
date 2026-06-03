"""Lake table specs for exchange schedule Bronze rows."""

from core.lake.schema import BronzeTableModel
from domains.exchange_schedule.models import ExchangeHolidaySnapshot, ExchangeScheduleSnapshot


class ExchangeScheduleTable(BronzeTableModel):
    """Physical schema for ``bronze.exchange_schedule``."""

    table_name = "exchange_schedule"
    row_model = ExchangeScheduleSnapshot
    unique_columns = ("snapshot_date", "provider_schedule_exchange_code", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_schedule_exchange_code")


class ExchangeHolidayTable(BronzeTableModel):
    """Physical schema for ``bronze.exchange_holiday``."""

    table_name = "exchange_holiday"
    row_model = ExchangeHolidaySnapshot
    unique_columns = ("snapshot_date", "provider_schedule_exchange_code", "holiday_date", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_schedule_exchange_code")


EXCHANGE_SCHEDULE_TABLE = ExchangeScheduleTable
EXCHANGE_HOLIDAY_TABLE = ExchangeHolidayTable

__all__ = [
    "EXCHANGE_HOLIDAY_TABLE",
    "EXCHANGE_SCHEDULE_TABLE",
    "ExchangeHolidayTable",
    "ExchangeScheduleTable",
]
