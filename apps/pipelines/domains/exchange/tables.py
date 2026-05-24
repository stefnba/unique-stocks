"""Lake table specs for exchange Bronze rows."""

from core.schema import BronzeTableModel
from domains.exchange.models import ExchangeSnapshot


class ExchangeTable(BronzeTableModel):
    """Physical schema for ``bronze.exchange``."""

    table_name = "exchange"
    row_model = ExchangeSnapshot
    unique_columns = ("snapshot_date", "exchange_code", "data_provider")
    idempotency_columns = ("snapshot_date",)


EXCHANGE_TABLE = ExchangeTable

__all__ = ["EXCHANGE_TABLE", "ExchangeTable"]
