"""Lake table specs for instrument Bronze rows."""

from core.schema import BronzeTableModel
from domains.instruments.models import InstrumentSnapshot


class InstrumentsTable(BronzeTableModel):
    """Physical schema for ``bronze.instruments``."""

    table_name = "instruments"
    row_model = InstrumentSnapshot
    unique_columns = ("snapshot_date", "exchange_code", "ticker", "provider")
    idempotency_columns = ("snapshot_date", "exchange_code")


INSTRUMENTS_TABLE = InstrumentsTable

__all__ = ["INSTRUMENTS_TABLE", "InstrumentsTable"]
