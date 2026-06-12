"""Shared read-model primitives for dashboard lake queries."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol, cast

from core.clients.lake.sql import SqlTemplateContext

_SQL_DIR = Path(__file__).with_name("sql")


class LakeReader(Protocol):
    """Read-only lake methods used by dashboard queries."""

    def table_exists(self, schema: str, table: str) -> bool:
        """Return whether a lake table exists."""
        ...

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Run a SELECT statement and return rows."""
        ...

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Run a SELECT statement and return one row."""
        ...

    def query_file(
        self,
        sql_path: str | Path,
        params: Sequence[Any] | None = None,
        *,
        template_context: SqlTemplateContext | None = None,
    ) -> list[dict[str, Any]]:
        """Run a SELECT statement from a SQL file and return rows."""
        ...

    def query_one_file(
        self,
        sql_path: str | Path,
        params: Sequence[Any] | None = None,
        *,
        template_context: SqlTemplateContext | None = None,
    ) -> dict[str, Any] | None:
        """Run a SELECT statement from a SQL file and return one row."""
        ...


def pipeline_runs_available(lake: LakeReader) -> bool:
    """Return whether the dashboard's base audit table exists."""
    return lake.table_exists("pipeline", "runs")


def run_units_available(lake: LakeReader) -> bool:
    """Return whether the dashboard's work-unit table exists."""
    return lake.table_exists("pipeline", "run_units")


def landing_objects_available(lake: LakeReader) -> bool:
    """Return whether the dashboard's landing-object table exists."""
    return lake.table_exists("pipeline", "landing_objects")


def _clean_values(values: Sequence[str]) -> list[str]:
    """Drop blank strings from dashboard filter inputs.

    Args:
        values: Raw string filter values from the UI.

    Returns:
        Non-empty trimmed strings.
    """
    return [str(value) for value in values if str(value).strip()]


def _placeholders(count: int) -> str:
    """Build a comma-separated SQL placeholder list.

    Args:
        count: Number of placeholders to generate.

    Returns:
        Placeholder string such as ``?, ?, ?``.
    """
    return ", ".join("?" for _ in range(count))


def _bounded_limit(value: int, *, default: int, maximum: int) -> int:
    """Clamp user-provided limits to safe dashboard bounds.

    Args:
        value: Requested row limit from the UI or caller.
        default: Fallback when ``value`` cannot be parsed as an integer.
        maximum: Hard upper bound enforced for lake queries.

    Returns:
        Integer limit between ``1`` and ``maximum``.
    """
    try:
        numeric = int(value)
    except TypeError, ValueError:
        return default
    return max(1, min(numeric, maximum))


def _empty_summary() -> dict[str, int]:
    """Return zeroed KPI counters for unmigrated or empty lakes.

    Returns:
        Empty run-summary counter mapping used by overview KPIs.
    """
    return {
        "total_runs": 0,
        "running_runs": 0,
        "completed_runs": 0,
        "partial_runs": 0,
        "failed_runs": 0,
        "attention_runs": 0,
        "units_failed": 0,
        "rows_written": 0,
        "rows_rejected": 0,
    }


def _empty_evidence_summary() -> dict[str, int]:
    """Return zeroed cross-table audit evidence counters."""
    return {
        "landing_objects": 0,
        "landing_bytes": 0,
        "rejection_samples": 0,
        "coverage_records": 0,
        "dbt_invocations": 0,
        "dbt_attention_invocations": 0,
        "dbt_attention_nodes": 0,
    }


def _int_value(value: object) -> int:
    """Coerce lake counter values to integers.

    Args:
        value: Raw scalar from a SQL aggregate.

    Returns:
        Integer value, defaulting to zero for ``None``.
    """
    if value is None:
        return 0
    return int(cast(Any, value))
