"""Shared constants for the pipeline audit dashboard."""

from __future__ import annotations

CACHE_TTL_SECONDS = 60
RUN_UNITS_LIMIT = 500
LANDING_OBJECTS_LIMIT = 200
REJECTIONS_LIMIT = 100
DBT_NODE_RESULTS_LIMIT = 300
ATTENTION_RUNS_LIMIT = 200

OVERVIEW_PAGE = "overview"
RUN_PAGE = "run"
UNIT_PAGE = "unit"

ATTENTION_STATUSES = frozenset({"failed", "partial"})
UNIT_ATTENTION_STATUSES = frozenset({"failed", "unsupported", "skipped"})
HEALTHY_RUN_STATUSES = frozenset({"completed", "partial", "skipped"})

INTEGER_TABLE_COLUMNS = frozenset(
    {
        "byte_count",
        "failures",
        "rows_affected",
        "rows_raw",
        "rows_rejected",
        "rows_valid",
        "rows_written",
        "units_failed",
        "units_succeeded",
        "units_total",
    }
)
FLOAT_TABLE_COLUMNS = frozenset({"execution_time"})
