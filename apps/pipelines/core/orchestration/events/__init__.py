"""Prefect event subsystem public API."""

from core.orchestration.events.contracts import APP_LABEL, PIPELINES_LABEL, PrefectEvent
from core.orchestration.events.emitters import (
    emit_coverage_gate_failure,
    emit_dbt_failure,
    emit_event,
    emit_ingestion_status,
    emit_pipeline_cancelled,
    emit_stale_runs,
    publish_ingestion_summary,
)

__all__ = [
    "APP_LABEL",
    "PIPELINES_LABEL",
    "PrefectEvent",
    "emit_coverage_gate_failure",
    "emit_dbt_failure",
    "emit_event",
    "emit_ingestion_status",
    "emit_pipeline_cancelled",
    "emit_stale_runs",
    "publish_ingestion_summary",
]
