"""Lake table specs for exchange schedule Bronze rows."""

from core.schema import BronzeTableModel
from domains.exchange_schedules.models import ExchangeHolidaySnapshot, ExchangeScheduleSnapshot


class ExchangeSchedulesTable(BronzeTableModel):
    """Physical schema for ``bronze.exchange_schedules``."""

    table_name = "exchange_schedules"
    row_model = ExchangeScheduleSnapshot
    unique_columns = ("snapshot_date", "exchange_code", "provider")
    idempotency_columns = ("snapshot_date", "exchange_code")


class ExchangeHolidaysTable(BronzeTableModel):
    """Physical schema for ``bronze.exchange_holidays``."""

    table_name = "exchange_holidays"
    row_model = ExchangeHolidaySnapshot
    unique_columns = ("snapshot_date", "exchange_code", "holiday_date", "provider")
    idempotency_columns = ("snapshot_date", "exchange_code")


EXCHANGE_SCHEDULES_TABLE = ExchangeSchedulesTable
EXCHANGE_HOLIDAYS_TABLE = ExchangeHolidaysTable

__all__ = [
    "EXCHANGE_HOLIDAYS_TABLE",
    "EXCHANGE_SCHEDULES_TABLE",
    "ExchangeHolidaysTable",
    "ExchangeSchedulesTable",
]
