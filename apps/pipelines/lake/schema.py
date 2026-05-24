"""Application-level lake table registry."""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel

from core.schema import INTEGER, TEXT, SqlColumn, TableModel
from core.schema import UUID as SQL_UUID
from domains.eod_price.tables import EOD_PRICE_TABLE
from domains.exchange.tables import EXCHANGE_TABLE
from domains.exchange_schedule.tables import EXCHANGE_HOLIDAY_TABLE, EXCHANGE_SCHEDULE_TABLE
from domains.instrument.tables import INSTRUMENT_TABLE


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
    EOD_PRICE_TABLE,
    EXCHANGE_TABLE,
    EXCHANGE_SCHEDULE_TABLE,
    EXCHANGE_HOLIDAY_TABLE,
    INSTRUMENT_TABLE,
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
