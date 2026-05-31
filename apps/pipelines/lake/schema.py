"""Application-level lake table registry."""

from datetime import date, datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel

from core.schema import BOOLEAN, DOUBLE, INTEGER, JSON, TEXT, VARCHAR, SqlColumn, TableModel
from core.schema import UUID as SQL_UUID
from domains.eod_price.tables import EOD_PRICE_TABLE
from domains.exchange.tables import EXCHANGE_CATALOG_TABLE, EXCHANGE_MIC_REGISTRY_TABLE
from domains.exchange_schedule.tables import EXCHANGE_HOLIDAY_TABLE, EXCHANGE_SCHEDULE_TABLE
from domains.fundamental.tables import (
    FUNDAMENTAL_DOCUMENT_TABLE,
    FUNDAMENTAL_ETF_HOLDING_TABLE,
    FUNDAMENTAL_ETF_IDENTITY_TABLE,
    FUNDAMENTAL_FUND_METRIC_FACT_TABLE,
    FUNDAMENTAL_INDEX_COMPONENT_TABLE,
    FUNDAMENTAL_INDEX_HISTORICAL_COMPONENT_TABLE,
    FUNDAMENTAL_INDEX_IDENTITY_TABLE,
    FUNDAMENTAL_MUTUAL_FUND_HOLDING_TABLE,
    FUNDAMENTAL_MUTUAL_FUND_IDENTITY_TABLE,
    FUNDAMENTAL_STATEMENT_FACT_TABLE,
    FUNDAMENTAL_STOCK_DIVIDEND_COUNT_TABLE,
    FUNDAMENTAL_STOCK_EARNINGS_FACT_TABLE,
    FUNDAMENTAL_STOCK_ESG_ACTIVITY_TABLE,
    FUNDAMENTAL_STOCK_HOLDER_TABLE,
    FUNDAMENTAL_STOCK_IDENTITY_TABLE,
    FUNDAMENTAL_STOCK_INSIDER_TRANSACTION_TABLE,
    FUNDAMENTAL_STOCK_METRIC_FACT_TABLE,
    FUNDAMENTAL_STOCK_OUTSTANDING_SHARES_TABLE,
    FUNDAMENTAL_STOCK_SHARES_STATS_TABLE,
    FUNDAMENTAL_STOCK_SPLITS_DIVIDENDS_TABLE,
)
from domains.instrument.tables import INSTRUMENT_TABLE


class PipelineRunRow(BaseModel):
    """One pipeline run tracking row."""

    run_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True, default="GEN_RANDOM_UUID()")] = None
    parent_run_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True)] = None
    prefect_flow_run_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True)] = None
    flow_name: str
    domain: str
    run_kind: str
    provider: str | None = None
    environment: str | None = None
    code_version: str | None = None
    parameters_json: Annotated[dict[str, object] | None, SqlColumn(JSON, nullable=True)] = None
    target_window_start: date | None = None
    target_window_end: date | None = None
    status: str
    started_at: datetime
    completed_at: datetime | None = None
    units_total: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    units_succeeded: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    units_failed: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    units_skipped: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    rows_raw: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    rows_valid: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    rows_rejected: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    rows_written: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    summary_json: Annotated[dict[str, object] | None, SqlColumn(JSON, nullable=True)] = None
    error_class: Annotated[str | None, SqlColumn(VARCHAR, nullable=True)] = None
    error_message: Annotated[str | None, SqlColumn(TEXT, nullable=True)] = None


class PipelineRunsTable(TableModel):
    """Physical schema for ``pipeline.runs``."""

    schema_name = "pipeline"
    table_name = "runs"
    row_model = PipelineRunRow
    unique_columns = ("run_id",)
    idempotency_columns = ()


class PipelineRunUnitRow(BaseModel):
    """One audited pipeline work unit, such as one exchange/date or ticker range."""

    unit_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True, default="GEN_RANDOM_UUID()")] = None
    run_id: Annotated[UUID, SqlColumn(SQL_UUID)]
    domain: str
    provider: str | None = None
    unit_type: str
    unit_key_hash: str
    unit_key_json: Annotated[dict[str, object], SqlColumn(JSON)]
    status: str
    reason: str | None = None
    source_uri: str | None = None
    rows_raw: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    rows_valid: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    rows_rejected: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    rows_written: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_class: Annotated[str | None, SqlColumn(VARCHAR, nullable=True)] = None
    error_message: Annotated[str | None, SqlColumn(TEXT, nullable=True)] = None


class PipelineRunUnitsTable(TableModel):
    """Physical schema for ``pipeline.run_units``."""

    schema_name = "pipeline"
    table_name = "run_units"
    row_model = PipelineRunUnitRow
    unique_columns = ("run_id", "unit_type", "unit_key_hash")
    idempotency_columns = ()


class PipelineLandingObjectRow(BaseModel):
    """One landing object produced or consumed by a pipeline run."""

    landing_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True, default="GEN_RANDOM_UUID()")] = None
    run_id: Annotated[UUID, SqlColumn(SQL_UUID)]
    unit_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True)] = None
    domain: str
    provider: str | None = None
    dataset: str
    source_uri: str
    partition_json: Annotated[dict[str, object] | None, SqlColumn(JSON, nullable=True)] = None
    rows_raw: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    byte_count: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    content_hash: str | None = None
    recorded_at: datetime


class PipelineLandingObjectsTable(TableModel):
    """Physical schema for ``pipeline.landing_objects``."""

    schema_name = "pipeline"
    table_name = "landing_objects"
    row_model = PipelineLandingObjectRow
    unique_columns = ("run_id", "source_uri")
    idempotency_columns = ()


class PipelineRejectionRow(BaseModel):
    """One parsed raw item rejected during ingestion."""

    rejection_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True, default="GEN_RANDOM_UUID()")] = None
    run_id: Annotated[UUID, SqlColumn(SQL_UUID)]
    unit_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True)] = None
    domain: str
    entity_key_json: Annotated[dict[str, object] | None, SqlColumn(JSON, nullable=True)] = None
    source_uri: str | None = None
    raw_hash: str
    reason: str
    error_class: Annotated[str | None, SqlColumn(VARCHAR, nullable=True)] = None
    error_message: Annotated[str | None, SqlColumn(TEXT, nullable=True)] = None
    raw_sample_json: Annotated[dict[str, object] | None, SqlColumn(JSON, nullable=True)] = None
    recorded_at: datetime


class PipelineRejectionsTable(TableModel):
    """Physical schema for ``pipeline.rejections``."""

    schema_name = "pipeline"
    table_name = "rejections"
    row_model = PipelineRejectionRow
    unique_columns = ("run_id", "domain", "raw_hash")
    idempotency_columns = ()


class PipelineDbtInvocationRow(BaseModel):
    """One dbt command invocation audited in the lake."""

    dbt_run_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True, default="GEN_RANDOM_UUID()")] = None
    run_id: Annotated[UUID, SqlColumn(SQL_UUID)]
    dbt_invocation_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True)] = None
    command: str
    command_args_json: Annotated[list[object], SqlColumn(JSON)]
    project_dir: str
    profiles_dir: str
    target: str | None = None
    status: str
    return_code: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    started_at: datetime
    completed_at: datetime | None = None
    elapsed_seconds: Annotated[float | None, SqlColumn(DOUBLE, nullable=True)] = None
    artifact_path: str | None = None
    artifact_metadata_json: Annotated[dict[str, object] | None, SqlColumn(JSON, nullable=True)] = None
    error_message: Annotated[str | None, SqlColumn(TEXT, nullable=True)] = None


class PipelineDbtInvocationsTable(TableModel):
    """Physical schema for ``pipeline.dbt_invocations``."""

    schema_name = "pipeline"
    table_name = "dbt_invocations"
    row_model = PipelineDbtInvocationRow
    unique_columns = ("dbt_run_id",)
    idempotency_columns = ()


class PipelineDbtNodeResultRow(BaseModel):
    """One node result from a dbt ``run_results.json`` artifact."""

    node_result_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True, default="GEN_RANDOM_UUID()")] = None
    dbt_run_id: Annotated[UUID, SqlColumn(SQL_UUID)]
    unique_id: str
    resource_type: str | None = None
    status: str
    execution_time: Annotated[float | None, SqlColumn(DOUBLE, nullable=True)] = None
    failures: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    message: Annotated[str | None, SqlColumn(TEXT, nullable=True)] = None
    adapter_response_json: Annotated[dict[str, object] | None, SqlColumn(JSON, nullable=True)] = None
    rows_affected: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    relation_name: str | None = None
    compiled: Annotated[bool | None, SqlColumn(BOOLEAN, nullable=True)] = None


class PipelineDbtNodeResultsTable(TableModel):
    """Physical schema for ``pipeline.dbt_node_results``."""

    schema_name = "pipeline"
    table_name = "dbt_node_results"
    row_model = PipelineDbtNodeResultRow
    unique_columns = ("dbt_run_id", "unique_id")
    idempotency_columns = ()


PIPELINE_RUNS_TABLE = PipelineRunsTable
PIPELINE_RUN_UNITS_TABLE = PipelineRunUnitsTable
PIPELINE_LANDING_OBJECTS_TABLE = PipelineLandingObjectsTable
PIPELINE_REJECTIONS_TABLE = PipelineRejectionsTable
PIPELINE_DBT_INVOCATIONS_TABLE = PipelineDbtInvocationsTable
PIPELINE_DBT_NODE_RESULTS_TABLE = PipelineDbtNodeResultsTable

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
    FUNDAMENTAL_STOCK_SPLITS_DIVIDENDS_TABLE,
    FUNDAMENTAL_STOCK_DIVIDEND_COUNT_TABLE,
    FUNDAMENTAL_STOCK_METRIC_FACT_TABLE,
    FUNDAMENTAL_STOCK_ESG_ACTIVITY_TABLE,
    FUNDAMENTAL_ETF_IDENTITY_TABLE,
    FUNDAMENTAL_MUTUAL_FUND_IDENTITY_TABLE,
    FUNDAMENTAL_INDEX_IDENTITY_TABLE,
    FUNDAMENTAL_ETF_HOLDING_TABLE,
    FUNDAMENTAL_MUTUAL_FUND_HOLDING_TABLE,
    FUNDAMENTAL_FUND_METRIC_FACT_TABLE,
    FUNDAMENTAL_INDEX_COMPONENT_TABLE,
    FUNDAMENTAL_INDEX_HISTORICAL_COMPONENT_TABLE,
)

PIPELINE_TABLES = (
    PIPELINE_RUNS_TABLE,
    PIPELINE_RUN_UNITS_TABLE,
    PIPELINE_LANDING_OBJECTS_TABLE,
    PIPELINE_REJECTIONS_TABLE,
    PIPELINE_DBT_INVOCATIONS_TABLE,
    PIPELINE_DBT_NODE_RESULTS_TABLE,
)

ALL_TABLES = (*BRONZE_TABLES, *PIPELINE_TABLES)


__all__ = [
    "ALL_TABLES",
    "BRONZE_TABLES",
    "PIPELINE_DBT_INVOCATIONS_TABLE",
    "PIPELINE_DBT_NODE_RESULTS_TABLE",
    "PIPELINE_LANDING_OBJECTS_TABLE",
    "PIPELINE_REJECTIONS_TABLE",
    "PIPELINE_RUN_UNITS_TABLE",
    "PIPELINE_RUNS_TABLE",
    "PIPELINE_TABLES",
    "PipelineDbtInvocationRow",
    "PipelineDbtInvocationsTable",
    "PipelineDbtNodeResultRow",
    "PipelineDbtNodeResultsTable",
    "PipelineLandingObjectRow",
    "PipelineLandingObjectsTable",
    "PipelineRejectionRow",
    "PipelineRejectionsTable",
    "PipelineRunUnitRow",
    "PipelineRunUnitsTable",
    "PipelineRunRow",
    "PipelineRunsTable",
]
