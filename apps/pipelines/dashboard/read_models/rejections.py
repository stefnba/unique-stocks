"""Parser rejection dashboard lake queries."""

from __future__ import annotations

from typing import Any

from dashboard.read_models.base import _SQL_DIR, LakeReader, _bounded_limit


def load_rejections(
    lake: LakeReader,
    *,
    run_id: str,
    unit_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Load sampled parser rejections linked to a selected run or work unit."""
    if not lake.table_exists("pipeline", "rejections"):
        return []
    clauses = ["run_id = ?"]
    params: list[Any] = [run_id]
    if unit_id:
        clauses.append("unit_id = ?")
        params.append(unit_id)
    params.append(_bounded_limit(limit, default=100, maximum=1000))
    return lake.query_file(
        _SQL_DIR / "load_rejections.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )
