"""Reusable lake infrastructure.

This package owns generic lake access: the DuckDB/MotherDuck client, SQL
template rendering, schema primitives, and migration helpers. It deliberately
does not register this app's concrete Bronze or pipeline tables; the pipelines
application composes those in ``lakehouse.schema``.
"""

from core.lake.client import (
    DataLakeClient,
    LakeDataFormat,
    LakeUpload,
    LakeWriteMode,
    get_lake_client,
    reset_lake_client,
)
from core.lake.models import BronzeModel
from core.lake.sql import SqlTemplateContext, render_sql, render_sql_file

__all__ = [
    "BronzeModel",
    "DataLakeClient",
    "LakeDataFormat",
    "LakeUpload",
    "LakeWriteMode",
    "SqlTemplateContext",
    "get_lake_client",
    "render_sql",
    "render_sql_file",
    "reset_lake_client",
]
