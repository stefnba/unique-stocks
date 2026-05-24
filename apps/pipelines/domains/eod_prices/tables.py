"""Lake table specs for EOD price Bronze rows."""

from core.schema import BronzeTableModel
from domains.eod_prices.models import EODBar


class EODPricesTable(BronzeTableModel):
    """Physical schema for ``bronze.eod_prices``."""

    table_name = "eod_prices"
    row_model = EODBar
    unique_columns = ("ticker", "bar_date", "provider")
    idempotency_columns = ("exchange_code", "bar_date")


EOD_PRICES_TABLE = EODPricesTable

__all__ = ["EODPricesTable", "EOD_PRICES_TABLE"]
