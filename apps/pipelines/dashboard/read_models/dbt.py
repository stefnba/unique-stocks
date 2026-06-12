"""dbt audit dashboard lake queries."""

from __future__ import annotations

from typing import Any

from dashboard.read_models.base import _SQL_DIR, LakeReader, _bounded_limit


def load_dbt_node_results(lake: LakeReader, *, run_id: str, limit: int = 300) -> list[dict[str, Any]]:
    """Load dbt model/test node results linked to a selected pipeline run."""
    if not lake.table_exists("pipeline", "dbt_invocations") or not lake.table_exists("pipeline", "dbt_node_results"):
        return []
    return lake.query_file(
        _SQL_DIR / "load_dbt_node_results.sql",
        [run_id, _bounded_limit(limit, default=300, maximum=2000)],
    )
