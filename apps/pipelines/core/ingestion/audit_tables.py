"""Core pipeline audit table specifications.

This module owns the physical schema for durable pipeline audit tables written
by generic core helpers such as run tracking, ingestion coverage, landing-object
recording, parser rejection recording, and dbt invocation auditing.

The application-level ``lake.schema`` registry imports these table specs into
``ALL_TABLES`` alongside domain Bronze tables. Core writers should depend on
this module directly instead of importing the app registry, which keeps the
dependency direction clean: app registries compose core/domain specs; core does
not know app registries.
"""

from datetime import date, datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel

from core.lake.schema import BOOLEAN, DOUBLE, INTEGER, JSON, TEXT, VARCHAR, SqlColumn, TableModel
from core.lake.schema import UUID as SQL_UUID


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
    """One audited pipeline work unit, such as one exchange/date or instrument range."""

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


class PipelineIngestionCoverageRow(BaseModel):
    """Terminal ingestion partition outcome for cross-run resume (not bronze data)."""

    coverage_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True, default="GEN_RANDOM_UUID()")] = None
    run_id: Annotated[UUID, SqlColumn(SQL_UUID)]
    domain: str
    provider: str
    unit_type: str
    unit_key_hash: str
    unit_key_json: Annotated[dict[str, object], SqlColumn(JSON)]
    status: str
    reason: str | None = None
    rows_raw: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    rows_valid: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    rows_rejected: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    source_uri: str | None = None
    recorded_at: datetime


class PipelineIngestionCoverageTable(TableModel):
    """Physical schema for ``pipeline.ingestion_coverage``."""

    schema_name = "pipeline"
    table_name = "ingestion_coverage"
    row_model = PipelineIngestionCoverageRow
    unique_columns = ("domain", "provider", "unit_type", "unit_key_hash", "status")
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
PIPELINE_INGESTION_COVERAGE_TABLE = PipelineIngestionCoverageTable
PIPELINE_LANDING_OBJECTS_TABLE = PipelineLandingObjectsTable
PIPELINE_REJECTIONS_TABLE = PipelineRejectionsTable
PIPELINE_DBT_INVOCATIONS_TABLE = PipelineDbtInvocationsTable
PIPELINE_DBT_NODE_RESULTS_TABLE = PipelineDbtNodeResultsTable

PIPELINE_TABLES = (
    PIPELINE_RUNS_TABLE,
    PIPELINE_RUN_UNITS_TABLE,
    PIPELINE_INGESTION_COVERAGE_TABLE,
    PIPELINE_LANDING_OBJECTS_TABLE,
    PIPELINE_REJECTIONS_TABLE,
    PIPELINE_DBT_INVOCATIONS_TABLE,
    PIPELINE_DBT_NODE_RESULTS_TABLE,
)


__all__ = [
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
