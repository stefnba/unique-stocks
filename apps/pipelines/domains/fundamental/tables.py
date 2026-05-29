"""Lake table specs for fundamentals Bronze rows."""

from core.schema import BronzeTableModel
from domains.fundamental.models import (
    FundamentalDocument,
    FundamentalStatementFact,
    FundamentalStockIdentitySnapshot,
)


class FundamentalDocumentTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_document``."""

    table_name = "fundamental_document"
    row_model = FundamentalDocument
    unique_columns = ("snapshot_date", "ticker", "data_provider")
    idempotency_columns = ("snapshot_date", "ticker")


class FundamentalStockIdentityTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_identity``."""

    table_name = "fundamental_stock_identity"
    row_model = FundamentalStockIdentitySnapshot
    unique_columns = ("snapshot_date", "ticker", "data_provider")
    idempotency_columns = ("snapshot_date", "ticker")


class FundamentalStatementFactTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_statement_fact``."""

    table_name = "fundamental_statement_fact"
    row_model = FundamentalStatementFact
    unique_columns = (
        "snapshot_date",
        "ticker",
        "statement_type",
        "period_type",
        "period_end_date",
        "metric_name",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "ticker")


FUNDAMENTAL_DOCUMENT_TABLE = FundamentalDocumentTable
FUNDAMENTAL_STOCK_IDENTITY_TABLE = FundamentalStockIdentityTable
FUNDAMENTAL_STATEMENT_FACT_TABLE = FundamentalStatementFactTable

__all__ = [
    "FUNDAMENTAL_DOCUMENT_TABLE",
    "FUNDAMENTAL_STATEMENT_FACT_TABLE",
    "FUNDAMENTAL_STOCK_IDENTITY_TABLE",
    "FundamentalDocumentTable",
    "FundamentalStatementFactTable",
    "FundamentalStockIdentityTable",
]
