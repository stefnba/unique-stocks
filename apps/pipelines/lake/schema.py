"""Application-level lake table registry."""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel

from core.schema import INTEGER, TEXT, SqlColumn, TableModel
from core.schema import UUID as SQL_UUID
from domains.exchanges.tables import EXCHANGES_TABLE


class PipelineRunRow(BaseModel):
    """One pipeline run tracking row."""

    run_id: Annotated[UUID | None, SqlColumn(SQL_UUID, nullable=True, default="GEN_RANDOM_UUID()")] = None
    flow_name: str
    status: str
    started_at: datetime
    completed_at: datetime | None = None
    rows_written: Annotated[int | None, SqlColumn(INTEGER, nullable=True)] = None
    error_message: Annotated[str | None, SqlColumn(TEXT, nullable=True)] = None


class PipelineRunsTable(TableModel):
    """Physical schema for ``pipeline.runs``."""

    schema_name = "pipeline"
    table_name = "runs"
    row_model = PipelineRunRow
    unique_columns = ("run_id",)
    idempotency_columns = ()


PIPELINE_RUNS_TABLE = PipelineRunsTable

BRONZE_TABLES = (
    EXCHANGES_TABLE,
)

PIPELINE_TABLES = (PIPELINE_RUNS_TABLE,)

ALL_TABLES = (*BRONZE_TABLES, *PIPELINE_TABLES)


__all__ = [
    "ALL_TABLES",
    "BRONZE_TABLES",
    "PIPELINE_RUNS_TABLE",
    "PIPELINE_TABLES",
    "PipelineRunRow",
    "PipelineRunsTable",
]
