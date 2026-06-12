"""Compatibility facade for dashboard read-model queries."""

from __future__ import annotations

from dashboard.constants import ATTENTION_STATUSES, DEFAULT_DASHBOARD_DOMAINS, HEALTHY_TERMINAL_STATUSES, RUN_STATUSES
from dashboard.read_models.base import (
    LakeReader,
    landing_objects_available,
    pipeline_runs_available,
    run_units_available,
)
from dashboard.read_models.dbt import load_dbt_node_results
from dashboard.read_models.evidence import load_audit_evidence_summary
from dashboard.read_models.landing_objects import (
    load_landing_object_by_id,
    load_landing_objects,
    load_recent_landing_objects,
)
from dashboard.read_models.rejections import load_rejections
from dashboard.read_models.run_units import (
    load_recent_run_units,
    load_run_unit_by_id,
    load_run_units,
    load_unit_status_breakdown,
)
from dashboard.read_models.runs import (
    load_attention_runs,
    load_daily_run_trend,
    load_domain_run_summary,
    load_latest_attention_runs_by_domain,
    load_latest_runs_by_domain,
    load_latest_terminal_runs_by_domain,
    load_recent_runs,
    load_run_by_id,
    load_stale_running_runs,
    load_status_breakdown,
    load_status_summary,
)

__all__ = [
    "ATTENTION_STATUSES",
    "DEFAULT_DASHBOARD_DOMAINS",
    "HEALTHY_TERMINAL_STATUSES",
    "LakeReader",
    "RUN_STATUSES",
    "landing_objects_available",
    "load_attention_runs",
    "load_audit_evidence_summary",
    "load_daily_run_trend",
    "load_dbt_node_results",
    "load_domain_run_summary",
    "load_landing_object_by_id",
    "load_landing_objects",
    "load_latest_attention_runs_by_domain",
    "load_latest_runs_by_domain",
    "load_latest_terminal_runs_by_domain",
    "load_recent_landing_objects",
    "load_recent_run_units",
    "load_recent_runs",
    "load_rejections",
    "load_run_by_id",
    "load_run_unit_by_id",
    "load_run_units",
    "load_stale_running_runs",
    "load_status_breakdown",
    "load_status_summary",
    "load_unit_status_breakdown",
    "pipeline_runs_available",
    "run_units_available",
]
