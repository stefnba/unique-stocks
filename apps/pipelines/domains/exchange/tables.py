"""Lake table specs for exchange Bronze rows."""

from core.lake.schema import BronzeTableModel
from domains.exchange.models import ExchangeCatalogSnapshot, ExchangeMicRegistrySnapshot


class ExchangeCatalogTable(BronzeTableModel):
    """Physical schema for ``bronze.exchange_catalog``."""

    table_name = "exchange_catalog"
    row_model = ExchangeCatalogSnapshot
    unique_columns = ("snapshot_date", "provider_exchange_code", "data_provider")
    idempotency_columns = ("snapshot_date",)


class ExchangeMicRegistryTable(BronzeTableModel):
    """Physical schema for ``bronze.exchange_mic_registry``."""

    table_name = "exchange_mic_registry"
    row_model = ExchangeMicRegistrySnapshot
    unique_columns = ("snapshot_date", "mic", "data_provider")
    idempotency_columns = ("snapshot_date",)


EXCHANGE_CATALOG_TABLE = ExchangeCatalogTable
EXCHANGE_MIC_REGISTRY_TABLE = ExchangeMicRegistryTable

__all__ = [
    "EXCHANGE_CATALOG_TABLE",
    "EXCHANGE_MIC_REGISTRY_TABLE",
    "ExchangeCatalogTable",
    "ExchangeMicRegistryTable",
]
