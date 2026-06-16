"""Public Prefect-backed event emitters for pipeline observability."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from prefect.events import emit_event as _emit_prefect_event

from core.orchestration.artifacts import create_ingestion_summary_artifact
from core.orchestration.events.contracts import PrefectEvent as _PrefectEvent
from core.orchestration.events.resources import (
    EventLabels,
    EventRelatedResource,
    app_run_related_resource,
    event_resource,
    related_resources,
)

logger = structlog.get_logger(__name__)
MAX_EVENT_COLLECTION_SAMPLE = 10


def emit_event(
    *,
    event: str,
    resource_id: str,
    resource_name: str,
    payload: dict[str, Any],
    labels: EventLabels | None = None,
    related: Sequence[EventRelatedResource] | None = None,
) -> None:
    """Emit one JSON-safe Prefect event for pipeline automations.

    The primary resource carries stable ``unique-stocks.*`` labels used by
    control-plane automations for matching and grouping. Related resources add
    navigable context, such as the current Prefect flow run or app run. Event
    publication is an observability side effect: failures are logged but never
    fail the caller's ingestion or transformation work.
    """
    try:
        event_related_resources = related_resources(related)
        _emit_prefect_event(
            event=event,
            resource=event_resource(
                resource_id=resource_id,
                resource_name=resource_name,
                labels=labels,
            ),
            related=event_related_resources or None,
            payload=_jsonable(payload),
        )
    except Exception as exc:
        logger.warning(
            "prefect_event_emit_failed",
            event=event,
            resource_id=resource_id,
            error=str(exc),
        )


def emit_dbt_failure(
    *,
    dbt_run_id: str,
    app_run_id: str | None,
    command: str,
    target: str,
    return_code: int,
    failed_nodes: int,
    artifact_path: str | None,
) -> None:
    """Emit the canonical Prefect event for a failed dbt invocation."""
    emit_event(
        event=_PrefectEvent.DBT_FAILED,
        resource_id=f"unique-stocks.dbt-invocation.{dbt_run_id}",
        resource_name=f"dbt {command}",
        labels={
            "unique-stocks.resource.kind": "dbt-invocation",
            "unique-stocks.app_run_id": app_run_id,
            "unique-stocks.dbt_run_id": dbt_run_id,
            "unique-stocks.dbt.command": command,
            "unique-stocks.dbt.target": target,
            "unique-stocks.status": "failed",
            "unique-stocks.severity": "critical",
        },
        related=[app_run_related_resource(app_run_id)] if app_run_id else None,
        payload={
            "dbt_run_id": dbt_run_id,
            "app_run_id": app_run_id,
            "command": command,
            "target": target,
            "return_code": return_code,
            "failed_nodes": failed_nodes,
            "artifact_path": artifact_path,
        },
    )


def emit_coverage_gate_failure(
    *,
    app_run_id: str,
    gaps_count: int,
    provider_exchange_codes: Sequence[str],
    from_date: str,
    to_date: str,
) -> None:
    """Emit the Prefect event for EOD price coverage gaps after ingestion."""
    emit_event(
        event=_PrefectEvent.COVERAGE_GATE_FAILED,
        resource_id=f"unique-stocks.coverage-gate.{app_run_id}",
        resource_name="EOD coverage gate",
        labels={
            "unique-stocks.resource.kind": "coverage-gate",
            "unique-stocks.app_run_id": app_run_id,
            "unique-stocks.domain": "eod_price",
            "unique-stocks.status": "failed",
            "unique-stocks.severity": "warning",
        },
        related=[app_run_related_resource(app_run_id)],
        payload={
            "app_run_id": app_run_id,
            "gaps_count": gaps_count,
            "provider_exchange_codes": list(provider_exchange_codes),
            "from_date": from_date,
            "to_date": to_date,
        },
    )


def emit_ingestion_status(
    *,
    flow_name: str,
    domain: str,
    app_run_id: str,
    status: str,
    summary: dict[str, Any],
    artifact_key: str | None = None,
) -> None:
    """Emit a Prefect alert event for terminal partial or failed ingestion."""
    if status not in {"partial", "failed"}:
        return
    event = _PrefectEvent.INGESTION_PARTIAL if status == "partial" else _PrefectEvent.INGESTION_FAILED
    emit_event(
        event=event,
        resource_id=f"unique-stocks.ingestion-run.{app_run_id}",
        resource_name=flow_name,
        labels={
            "unique-stocks.resource.kind": "ingestion-run",
            "unique-stocks.app_run_id": app_run_id,
            "unique-stocks.flow_name": flow_name,
            "unique-stocks.domain": domain,
            "unique-stocks.status": status,
            "unique-stocks.severity": "critical" if status == "failed" else "warning",
        },
        related=[app_run_related_resource(app_run_id, flow_name=flow_name, domain=domain)],
        payload={
            "app_run_id": app_run_id,
            "flow_name": flow_name,
            "domain": domain,
            "status": status,
            "artifact_key": artifact_key,
            "summary": _compact_event_summary(summary),
            "summary_source": "pipeline.runs.summary_json",
        },
    )


def emit_stale_runs(
    *,
    stale_runs: Sequence[dict[str, Any]],
    older_than_minutes: int,
) -> None:
    """Emit the Prefect audit event for pipeline runs stuck in running state."""
    emit_event(
        event=_PrefectEvent.PIPELINE_STALE_RUNNING,
        resource_id="unique-stocks.pipeline.runs",
        resource_name="Stale pipeline runs",
        labels={
            "unique-stocks.resource.kind": "pipeline-runs",
            "unique-stocks.status": "stale-running",
            "unique-stocks.severity": "critical",
        },
        payload={
            "older_than_minutes": older_than_minutes,
            "stale_run_count": len(stale_runs),
            "stale_runs_sample": list(stale_runs[:MAX_EVENT_COLLECTION_SAMPLE]),
            "stale_runs_truncated": len(stale_runs) > MAX_EVENT_COLLECTION_SAMPLE,
        },
    )


def emit_pipeline_cancelled(
    *,
    app_run_id: str,
    flow_name: str,
    error_class: str,
    error_message: str,
) -> None:
    """Emit the Prefect audit event for a pipeline run cancelled by orchestration."""
    emit_event(
        event=_PrefectEvent.PIPELINE_CANCELLED,
        resource_id=f"unique-stocks.pipeline-run.{app_run_id}",
        resource_name=flow_name,
        labels={
            "unique-stocks.resource.kind": "pipeline-run",
            "unique-stocks.app_run_id": app_run_id,
            "unique-stocks.flow_name": flow_name,
            "unique-stocks.status": "cancelled",
            "unique-stocks.severity": "warning",
        },
        related=[app_run_related_resource(app_run_id, flow_name=flow_name)],
        payload={
            "app_run_id": app_run_id,
            "flow_name": flow_name,
            "error_class": error_class,
            "error_message": error_message,
        },
    )


async def publish_ingestion_summary(
    *,
    flow_name: str,
    domain: str,
    app_run_id: str,
    status: str,
    summary: dict[str, Any],
) -> None:
    """Create a Prefect summary artifact and emit a partial/failed event if needed."""
    artifact_key = await create_ingestion_summary_artifact(
        flow_name=flow_name,
        domain=domain,
        app_run_id=app_run_id,
        status=status,
        summary=summary,
    )
    emit_ingestion_status(
        flow_name=flow_name,
        domain=domain,
        app_run_id=app_run_id,
        status=status,
        summary=summary,
        artifact_key=artifact_key,
    )


def _compact_event_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, value in summary.items():
        if _is_event_scalar(value):
            compact[key] = _jsonable(value)
        elif isinstance(value, Mapping):
            compact[f"{key}_count"] = len(value)
            scalar_items = {
                str(item_key): _jsonable(item_value)
                for item_key, item_value in value.items()
                if _is_event_scalar(item_value)
            }
            if scalar_items:
                compact[key] = scalar_items
        elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray | memoryview):
            compact[f"{key}_count"] = len(value)
            if value:
                compact[f"{key}_sample"] = _jsonable(list(value[:MAX_EVENT_COLLECTION_SAMPLE]))
                compact[f"{key}_truncated"] = len(value) > MAX_EVENT_COLLECTION_SAMPLE
        else:
            compact[key] = str(value)
    return compact


def _is_event_scalar(value: object) -> bool:
    return value is None or isinstance(value, str | int | float | bool | Decimal | date | datetime | UUID)


def _jsonable(value: Any) -> Any:
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray | memoryview):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date | datetime | UUID):
        return str(value)
    return value


__all__ = [
    "emit_coverage_gate_failure",
    "emit_dbt_failure",
    "emit_event",
    "emit_ingestion_status",
    "emit_pipeline_cancelled",
    "emit_stale_runs",
    "publish_ingestion_summary",
]
