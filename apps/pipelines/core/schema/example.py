from datetime import date

from core.models import BronzeModel
from core.schema.table import BronzeTableModel


class ExchangeSnapshot(BronzeModel):
    """One exchange reference row for a snapshot date."""

    snapshot_date: date
    exchange_code: str
    name: str
    operating_mic: str | None = None
    country: str
    currency: str
    country_iso2: str
    country_iso3: str


class ExampleTableModel(BronzeTableModel):
    """Example table with intentionally invalid metadata names."""

    table_name = "exchange"
    row_model = ExchangeSnapshot
    unique_columns = ("snapshot_date", "exchange_code")
    idempotency_columns = ("snapshot_date",)


print(ExampleTableModel.to_ddl())
