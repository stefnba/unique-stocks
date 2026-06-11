"""Lake table specs for fundamentals Bronze rows."""

from core.lake.schema import BronzeTableModel
from domains.fundamental.models import (
    FundamentalDocument,
    FundamentalEtfHolding,
    FundamentalEtfIdentitySnapshot,
    FundamentalFundMetricFact,
    FundamentalIndexComponent,
    FundamentalIndexHistoricalComponent,
    FundamentalIndexIdentitySnapshot,
    FundamentalMutualFundHolding,
    FundamentalMutualFundIdentitySnapshot,
    FundamentalStatementFact,
    FundamentalStockDividendCount,
    FundamentalStockEarningsFact,
    FundamentalStockEsgActivity,
    FundamentalStockHolder,
    FundamentalStockIdentitySnapshot,
    FundamentalStockInsiderTransaction,
    FundamentalStockMetricFact,
    FundamentalStockOutstandingShares,
    FundamentalStockSharesStatsSnapshot,
    FundamentalStockSplitsDividendsSnapshot,
)


class FundamentalDocumentTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_document``."""

    table_name = "fundamental_document"
    row_model = FundamentalDocument
    unique_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStockIdentityTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_identity``."""

    table_name = "fundamental_stock_identity"
    row_model = FundamentalStockIdentitySnapshot
    unique_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStatementFactTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_statement_fact``."""

    table_name = "fundamental_statement_fact"
    row_model = FundamentalStatementFact
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "statement_type",
        "period_type",
        "period_end_date",
        "metric_name",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStockEarningsFactTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_earnings_fact``."""

    table_name = "fundamental_stock_earnings_fact"
    row_model = FundamentalStockEarningsFact
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "earnings_section",
        "period_type",
        "fiscal_period_end",
        "metric_name",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStockSharesStatsTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_shares_stats``."""

    table_name = "fundamental_stock_shares_stats"
    row_model = FundamentalStockSharesStatsSnapshot
    unique_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStockOutstandingSharesTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_outstanding_shares``."""

    table_name = "fundamental_stock_outstanding_shares"
    row_model = FundamentalStockOutstandingShares
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "period_type",
        "period_end_date",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStockHolderTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_holder``."""

    table_name = "fundamental_stock_holder"
    row_model = FundamentalStockHolder
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "holder_type",
        "holder_name",
        "report_date",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStockInsiderTransactionTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_insider_transaction``."""

    table_name = "fundamental_stock_insider_transaction"
    row_model = FundamentalStockInsiderTransaction
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "provider_position",
        "owner_name",
        "transaction_date",
        "transaction_code",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStockSplitsDividendsTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_splits_dividends``."""

    table_name = "fundamental_stock_splits_dividends"
    row_model = FundamentalStockSplitsDividendsSnapshot
    unique_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStockDividendCountTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_dividend_count``."""

    table_name = "fundamental_stock_dividend_count"
    row_model = FundamentalStockDividendCount
    unique_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code", "year", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStockMetricFactTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_metric_fact``."""

    table_name = "fundamental_stock_metric_fact"
    row_model = FundamentalStockMetricFact
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "metric_group",
        "metric_name",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalStockEsgActivityTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_stock_esg_activity``."""

    table_name = "fundamental_stock_esg_activity"
    row_model = FundamentalStockEsgActivity
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "activity",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalEtfIdentityTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_etf_identity``."""

    table_name = "fundamental_etf_identity"
    row_model = FundamentalEtfIdentitySnapshot
    unique_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalMutualFundIdentityTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_mutual_fund_identity``."""

    table_name = "fundamental_mutual_fund_identity"
    row_model = FundamentalMutualFundIdentitySnapshot
    unique_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalIndexIdentityTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_index_identity``."""

    table_name = "fundamental_index_identity"
    row_model = FundamentalIndexIdentitySnapshot
    unique_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code", "data_provider")
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalEtfHoldingTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_etf_holding``."""

    table_name = "fundamental_etf_holding"
    row_model = FundamentalEtfHolding
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "holding_provider_key",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalMutualFundHoldingTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_mutual_fund_holding``."""

    table_name = "fundamental_mutual_fund_holding"
    row_model = FundamentalMutualFundHolding
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "provider_position",
        "holding_name",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalFundMetricFactTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_fund_metric_fact``."""

    table_name = "fundamental_fund_metric_fact"
    row_model = FundamentalFundMetricFact
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "instrument_family",
        "metric_group",
        "metric_category",
        "metric_name",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalIndexComponentTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_index_component``."""

    table_name = "fundamental_index_component"
    row_model = FundamentalIndexComponent
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "component_provider_instrument_code",
        "component_provider_exchange_code",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


class FundamentalIndexHistoricalComponentTable(BronzeTableModel):
    """Physical schema for ``bronze.fundamental_index_historical_component``."""

    table_name = "fundamental_index_historical_component"
    row_model = FundamentalIndexHistoricalComponent
    unique_columns = (
        "snapshot_date",
        "provider_exchange_code",
        "provider_instrument_code",
        "component_provider_instrument_code",
        "start_date",
        "data_provider",
    )
    idempotency_columns = ("snapshot_date", "provider_exchange_code", "provider_instrument_code")


FUNDAMENTAL_DOCUMENT_TABLE = FundamentalDocumentTable
FUNDAMENTAL_STOCK_IDENTITY_TABLE = FundamentalStockIdentityTable
FUNDAMENTAL_STATEMENT_FACT_TABLE = FundamentalStatementFactTable
FUNDAMENTAL_STOCK_EARNINGS_FACT_TABLE = FundamentalStockEarningsFactTable
FUNDAMENTAL_STOCK_SHARES_STATS_TABLE = FundamentalStockSharesStatsTable
FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_TABLE = FundamentalStockOutstandingSharesTable
FUNDAMENTAL_STOCK_HOLDER_TABLE = FundamentalStockHolderTable
FUNDAMENTAL_STOCK_INSIDER_TRANSACTION_TABLE = FundamentalStockInsiderTransactionTable
FUNDAMENTAL_STOCK_SPLITS_DIVIDENDS_TABLE = FundamentalStockSplitsDividendsTable
FUNDAMENTAL_STOCK_DIVIDEND_COUNT_TABLE = FundamentalStockDividendCountTable
FUNDAMENTAL_STOCK_METRIC_FACT_TABLE = FundamentalStockMetricFactTable
FUNDAMENTAL_STOCK_ESG_ACTIVITY_TABLE = FundamentalStockEsgActivityTable
FUNDAMENTAL_ETF_IDENTITY_TABLE = FundamentalEtfIdentityTable
FUNDAMENTAL_MUTUAL_FUND_IDENTITY_TABLE = FundamentalMutualFundIdentityTable
FUNDAMENTAL_INDEX_IDENTITY_TABLE = FundamentalIndexIdentityTable
FUNDAMENTAL_ETF_HOLDING_TABLE = FundamentalEtfHoldingTable
FUNDAMENTAL_MUTUAL_FUND_HOLDING_TABLE = FundamentalMutualFundHoldingTable
FUNDAMENTAL_FUND_METRIC_FACT_TABLE = FundamentalFundMetricFactTable
FUNDAMENTAL_INDEX_COMPONENT_TABLE = FundamentalIndexComponentTable
FUNDAMENTAL_INDEX_HISTORICAL_COMPONENT_TABLE = FundamentalIndexHistoricalComponentTable

__all__ = [
    "FUNDAMENTAL_ETF_HOLDING_TABLE",
    "FUNDAMENTAL_ETF_IDENTITY_TABLE",
    "FUNDAMENTAL_DOCUMENT_TABLE",
    "FUNDAMENTAL_FUND_METRIC_FACT_TABLE",
    "FUNDAMENTAL_INDEX_COMPONENT_TABLE",
    "FUNDAMENTAL_INDEX_HISTORICAL_COMPONENT_TABLE",
    "FUNDAMENTAL_INDEX_IDENTITY_TABLE",
    "FUNDAMENTAL_MUTUAL_FUND_HOLDING_TABLE",
    "FUNDAMENTAL_MUTUAL_FUND_IDENTITY_TABLE",
    "FUNDAMENTAL_STATEMENT_FACT_TABLE",
    "FUNDAMENTAL_STOCK_DIVIDEND_COUNT_TABLE",
    "FUNDAMENTAL_STOCK_EARNINGS_FACT_TABLE",
    "FUNDAMENTAL_STOCK_ESG_ACTIVITY_TABLE",
    "FUNDAMENTAL_STOCK_HOLDER_TABLE",
    "FUNDAMENTAL_STOCK_IDENTITY_TABLE",
    "FUNDAMENTAL_STOCK_INSIDER_TRANSACTION_TABLE",
    "FUNDAMENTAL_STOCK_METRIC_FACT_TABLE",
    "FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_TABLE",
    "FUNDAMENTAL_STOCK_SHARES_STATS_TABLE",
    "FUNDAMENTAL_STOCK_SPLITS_DIVIDENDS_TABLE",
    "FundamentalEtfHoldingTable",
    "FundamentalEtfIdentityTable",
    "FundamentalDocumentTable",
    "FundamentalFundMetricFactTable",
    "FundamentalIndexComponentTable",
    "FundamentalIndexHistoricalComponentTable",
    "FundamentalIndexIdentityTable",
    "FundamentalMutualFundHoldingTable",
    "FundamentalMutualFundIdentityTable",
    "FundamentalStatementFactTable",
    "FundamentalStockDividendCountTable",
    "FundamentalStockEarningsFactTable",
    "FundamentalStockEsgActivityTable",
    "FundamentalStockHolderTable",
    "FundamentalStockIdentityTable",
    "FundamentalStockInsiderTransactionTable",
    "FundamentalStockMetricFactTable",
    "FundamentalStockOutstandingSharesTable",
    "FundamentalStockSharesStatsTable",
    "FundamentalStockSplitsDividendsTable",
]
