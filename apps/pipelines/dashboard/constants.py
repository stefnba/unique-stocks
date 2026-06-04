"""Shared constants for the pipeline audit dashboard."""

from __future__ import annotations

CACHE_TTL_SECONDS = 60
RUN_BROWSER_LIMIT = 1000
RUN_UNIT_BROWSER_LIMIT = 1000
LANDING_OBJECT_BROWSER_LIMIT = 1000
TABLE_PAGE_SIZE_OPTIONS = (25, 50, 100, 250)
RUN_UNITS_LIMIT = 500
LANDING_OBJECTS_LIMIT = 200
REJECTIONS_LIMIT = 100
DBT_NODE_RESULTS_LIMIT = 300
ATTENTION_RUNS_LIMIT = 200

OVERVIEW_PAGE = "overview"
DOMAINS_PAGE = "domains"
DOMAIN_DETAIL_PAGE = "domain-detail"
RUNS_PAGE = "runs"
RUN_DETAIL_PAGE = "run-detail"
RUN_UNITS_PAGE = "run-units"
RUN_UNIT_DETAIL_PAGE = "run-unit-detail"
LANDING_OBJECTS_PAGE = "landing-objects"
LANDING_OBJECT_DETAIL_PAGE = "landing-object-detail"

ATTENTION_STATUSES = frozenset({"failed", "partial"})
UNIT_ATTENTION_STATUSES = frozenset({"failed", "unsupported", "skipped"})
HEALTHY_RUN_STATUSES = frozenset({"completed", "partial", "skipped"})
UNIT_STATUSES = ("running", "completed", "failed", "unsupported", "skipped")

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
LINK_COLUMN_LABEL_PATTERN = r"#(.*)$"
