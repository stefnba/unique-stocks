"""Cross-table audit evidence summary queries."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from dashboard.read_models.base import (
    _SQL_DIR,
    LakeReader,
    _clean_values,
    _empty_evidence_summary,
    _int_value,
    _placeholders,
    pipeline_runs_available,
)


def load_audit_evidence_summary(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
) -> dict[str, int]:
    """Load compact cross-table audit evidence counters for the overview page."""
    summary = _empty_evidence_summary()
    domain_values = _clean_values(domains)

    if lake.table_exists("pipeline", "landing_objects"):
        clauses = ["recorded_at >= ?"]
        params: list[Any] = [since]
        if domain_values:
            clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one_file(
            _SQL_DIR / "load_landing_evidence_summary.sql",
            params,
            template_context={"where_clauses": " AND ".join(clauses)},
        )
        if row:
            summary["landing_objects"] = _int_value(row.get("landing_objects"))
            summary["landing_bytes"] = _int_value(row.get("landing_bytes"))

    if lake.table_exists("pipeline", "rejections"):
        clauses = ["recorded_at >= ?"]
        params = [since]
        if domain_values:
            clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one_file(
            _SQL_DIR / "load_rejection_evidence_summary.sql",
            params,
            template_context={"where_clauses": " AND ".join(clauses)},
        )
        if row:
            summary["rejection_samples"] = _int_value(row.get("rejection_samples"))

    if lake.table_exists("pipeline", "ingestion_coverage"):
        clauses = ["recorded_at >= ?"]
        params = [since]
        if domain_values:
            clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one_file(
            _SQL_DIR / "load_coverage_evidence_summary.sql",
            params,
            template_context={"where_clauses": " AND ".join(clauses)},
        )
        if row:
            summary["coverage_records"] = _int_value(row.get("coverage_records"))

    if lake.table_exists("pipeline", "dbt_invocations") and pipeline_runs_available(lake):
        clauses = ["run.started_at >= ?"]
        params = [since]
        if domain_values:
            clauses.append(f"run.domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one_file(
            _SQL_DIR / "load_dbt_invocation_evidence_summary.sql",
            params,
            template_context={"where_clauses": " AND ".join(clauses)},
        )
        if row:
            summary["dbt_invocations"] = _int_value(row.get("dbt_invocations"))
            summary["dbt_attention_invocations"] = _int_value(row.get("dbt_attention_invocations"))

    if (
        lake.table_exists("pipeline", "dbt_invocations")
        and lake.table_exists("pipeline", "dbt_node_results")
        and pipeline_runs_available(lake)
    ):
        clauses = ["run.started_at >= ?"]
        params = [since]
        if domain_values:
            clauses.append(f"run.domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one_file(
            _SQL_DIR / "load_dbt_attention_node_evidence_summary.sql",
            params,
            template_context={"where_clauses": " AND ".join(clauses)},
        )
        if row:
            summary["dbt_attention_nodes"] = _int_value(row.get("dbt_attention_nodes"))

    return summary
