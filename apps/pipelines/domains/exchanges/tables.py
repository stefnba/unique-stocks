"""Lake table specs for exchange Bronze rows."""

from core.schema import BronzeTableModel
from domains.exchanges.models import ExchangeSnapshot


class ExchangesTable(BronzeTableModel):
    """Physical schema for ``bronze.exchanges``."""

    table_name = "exchanges"
    row_model = ExchangeSnapshot
    unique_columns = ("snapshot_date", "exchange_code", "provider")
    idempotency_columns = ("snapshot_date",)


EXCHANGES_TABLE = ExchangesTable

__all__ = ["EXCHANGES_TABLE", "ExchangesTable"]
