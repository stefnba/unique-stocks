"""Lake table specs for EOD price Bronze rows."""

from core.schema import BronzeTableModel
from domains.eod_price.models import EODBar


class EODPriceTable(BronzeTableModel):
    """Physical schema for ``bronze.eod_price``."""

    table_name = "eod_price"
    row_model = EODBar
    unique_columns = ("ticker", "bar_date", "data_provider")
    idempotency_columns = ("provider_exchange_code", "bar_date")


EOD_PRICE_TABLE = EODPriceTable

__all__ = ["EODPriceTable", "EOD_PRICE_TABLE"]
