"""Bronze models for exchange schedule and holiday."""

from datetime import date

from core.models import BronzeModel


class ExchangeScheduleSnapshot(BronzeModel):
    """One EODHD v2 exchange-details schedule row for a snapshot date.

    ``provider_schedule_exchange_code`` is the v2 endpoint code. It is stored
    separately from official MIC metadata because EODHD v2 codes are not always
    MICs.
    """

    snapshot_date: date
    provider_schedule_exchange_code: str
    name: str
    timezone: str
    session_open: str
    session_close: str
    working_days: str
    pre_market_open: str | None = None
    pre_market_close: str | None = None
    after_hours_open: str | None = None
    after_hours_close: str | None = None
    lunch_break_start: str | None = None
    lunch_break_end: str | None = None


class ExchangeHolidaySnapshot(BronzeModel):
    """One holiday row from EODHD v2 exchange-details for a snapshot date."""

    snapshot_date: date
    provider_schedule_exchange_code: str
    holiday_date: date
    holiday_name: str
    holiday_type: str
    early_close_time: str | None = None
