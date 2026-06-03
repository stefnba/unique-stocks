"""Lake table specs for instrument Bronze rows."""

from core.lake.schema import BronzeTableModel
from domains.instrument.models import InstrumentSnapshot


class InstrumentTable(BronzeTableModel):
    """Physical schema for ``bronze.instrument``."""

    table_name = "instrument"
    row_model = InstrumentSnapshot
    unique_columns = ("snapshot_date", "provider_exchange_code", "ticker", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_exchange_code")


INSTRUMENT_TABLE = InstrumentTable

__all__ = ["INSTRUMENT_TABLE", "InstrumentTable"]
