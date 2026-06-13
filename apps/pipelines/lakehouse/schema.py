"""Application-level lake table registry.

This module composes the concrete table specs that belong to the pipelines app:
domain-owned Bronze tables plus core-owned pipeline audit tables. It should stay
as a registry and avoid defining reusable core schemas directly.
"""

from core.ingestion.audit_tables import (
    PIPELINE_DBT_INVOCATIONS_TABLE,
    PIPELINE_DBT_NODE_RESULTS_TABLE,
    PIPELINE_INGESTION_COVERAGE_TABLE,
    PIPELINE_LANDING_OBJECTS_TABLE,
    PIPELINE_REJECTIONS_TABLE,
    PIPELINE_RUN_UNITS_TABLE,
    PIPELINE_RUNS_TABLE,
    PIPELINE_TABLES,
    PipelineDbtInvocationRow,
    PipelineDbtInvocationsTable,
    PipelineDbtNodeResultRow,
    PipelineDbtNodeResultsTable,
    PipelineIngestionCoverageRow,
    PipelineIngestionCoverageTable,
    PipelineLandingObjectRow,
    PipelineLandingObjectsTable,
    PipelineRejectionRow,
    PipelineRejectionsTable,
    PipelineRunRow,
    PipelineRunsTable,
    PipelineRunUnitRow,
    PipelineRunUnitsTable,
)
from domains.eod_price.tables import EOD_PRICE_TABLE
from domains.exchange.tables import EXCHANGE_CATALOG_TABLE, EXCHANGE_MIC_REGISTRY_TABLE
from domains.exchange_schedule.tables import EXCHANGE_HOLIDAY_TABLE, EXCHANGE_SCHEDULE_TABLE
from domains.fundamental.tables import (
    FUNDAMENTAL_DOCUMENT_TABLE,
    FUNDAMENTAL_ETF_HOLDING_TABLE,
    FUNDAMENTAL_ETF_IDENTITY_TABLE,
    FUNDAMENTAL_FUND_METRIC_FACT_TABLE,
    FUNDAMENTAL_INDEX_COMPONENT_TABLE,
    FUNDAMENTAL_INDEX_IDENTITY_TABLE,
    FUNDAMENTAL_MUTUAL_FUND_HOLDING_TABLE,
    FUNDAMENTAL_MUTUAL_FUND_IDENTITY_TABLE,
    FUNDAMENTAL_STATEMENT_FACT_TABLE,
    FUNDAMENTAL_STOCK_EARNINGS_FACT_TABLE,
    FUNDAMENTAL_STOCK_HOLDER_TABLE,
    FUNDAMENTAL_STOCK_IDENTITY_TABLE,
    FUNDAMENTAL_STOCK_INSIDER_TRANSACTION_TABLE,
    FUNDAMENTAL_STOCK_METRIC_FACT_TABLE,
    FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_TABLE,
    FUNDAMENTAL_STOCK_SHARES_STATS_TABLE,
)
from domains.instrument.tables import INSTRUMENT_TABLE

BRONZE_TABLES = (
    EOD_PRICE_TABLE,
    EXCHANGE_CATALOG_TABLE,
    EXCHANGE_MIC_REGISTRY_TABLE,
    EXCHANGE_SCHEDULE_TABLE,
    EXCHANGE_HOLIDAY_TABLE,
    INSTRUMENT_TABLE,
    FUNDAMENTAL_DOCUMENT_TABLE,
    FUNDAMENTAL_STOCK_IDENTITY_TABLE,
    FUNDAMENTAL_STATEMENT_FACT_TABLE,
    FUNDAMENTAL_STOCK_EARNINGS_FACT_TABLE,
    FUNDAMENTAL_STOCK_SHARES_STATS_TABLE,
    FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_TABLE,
    FUNDAMENTAL_STOCK_HOLDER_TABLE,
    FUNDAMENTAL_STOCK_INSIDER_TRANSACTION_TABLE,
    FUNDAMENTAL_STOCK_METRIC_FACT_TABLE,
    FUNDAMENTAL_ETF_IDENTITY_TABLE,
    FUNDAMENTAL_MUTUAL_FUND_IDENTITY_TABLE,
    FUNDAMENTAL_INDEX_IDENTITY_TABLE,
    FUNDAMENTAL_ETF_HOLDING_TABLE,
    FUNDAMENTAL_MUTUAL_FUND_HOLDING_TABLE,
    FUNDAMENTAL_FUND_METRIC_FACT_TABLE,
    FUNDAMENTAL_INDEX_COMPONENT_TABLE,
)

ALL_TABLES = (*BRONZE_TABLES, *PIPELINE_TABLES)


__all__ = [
    "ALL_TABLES",
    "BRONZE_TABLES",
    "PIPELINE_DBT_INVOCATIONS_TABLE",
    "PIPELINE_DBT_NODE_RESULTS_TABLE",
    "PIPELINE_INGESTION_COVERAGE_TABLE",
    "PIPELINE_LANDING_OBJECTS_TABLE",
    "PIPELINE_REJECTIONS_TABLE",
    "PIPELINE_RUN_UNITS_TABLE",
    "PIPELINE_RUNS_TABLE",
    "PIPELINE_TABLES",
    "PipelineDbtInvocationRow",
    "PipelineDbtInvocationsTable",
    "PipelineDbtNodeResultRow",
    "PipelineDbtNodeResultsTable",
    "PipelineIngestionCoverageRow",
    "PipelineIngestionCoverageTable",
    "PipelineLandingObjectRow",
    "PipelineLandingObjectsTable",
    "PipelineRejectionRow",
    "PipelineRejectionsTable",
    "PipelineRunRow",
    "PipelineRunUnitRow",
    "PipelineRunUnitsTable",
    "PipelineRunsTable",
]
