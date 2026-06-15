"""Orchestration event vocabulary and Prefect-backed publishing helpers."""

from __future__ import annotations

import inspect
import json
import os
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

import structlog
from prefect.artifacts import create_markdown_artifact
from prefect.context import FlowRunContext, TaskRunContext, get_run_context
from prefect.events import emit_event

logger = structlog.get_logger(__name__)

type EventLabels = Mapping[str, object | None]
type EventRelatedResource = Mapping[str, object | None]

APP_LABEL = "unique-stocks"
PIPELINES_LABEL = "pipelines"
MAX_EVENT_COLLECTION_SAMPLE = 10


class PrefectEvent(StrEnum):
    """Canonical Prefect event names emitted and automated by this app."""

    DBT_FAILED = "unique-stocks.dbt.failed"
    COVERAGE_GATE_FAILED = "unique-stocks.coverage-gate.failed"
    INGESTION_PARTIAL = "unique-stocks.ingestion.partial"
    INGESTION_FAILED = "unique-stocks.ingestion.failed"
    PIPELINE_STALE_RUNNING = "unique-stocks.pipeline.stale-running"
    PIPELINE_CANCELLED = "unique-stocks.pipeline.cancelled"


def emit_prefect_event(
    *,
    event: str,
    resource_id: str,
    resource_name: str,
    payload: dict[str, Any],
    labels: EventLabels | None = None,
    related: Sequence[EventRelatedResource] | None = None,
) -> None:
    """Emit one JSON-safe Prefect event for pipeline automations.

    Event publication is an observability side effect: failures are logged but
    do not fail the caller's ingestion or transformation work.
    """
    try:
        related_resources = _related_resources(related)
        emit_event(
            event=event,
            resource={
                "prefect.resource.id": resource_id,
                "prefect.resource.name": resource_name,
                **_base_resource_labels(labels),
            },
            related=related_resources or None,
            payload=_jsonable(payload),
        )
    except Exception as exc:
        logger.warning(
            "prefect_event_emit_failed",
            event=event,
            resource_id=resource_id,
            error=str(exc),
        )


def emit_prefect_dbt_failure_event(
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
    emit_prefect_event(
        event=PrefectEvent.DBT_FAILED,
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
        related=[_app_run_related_resource(app_run_id)] if app_run_id else None,
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


def emit_prefect_coverage_gate_failure_event(
    *,
    app_run_id: str,
    gaps_count: int,
    provider_exchange_codes: Sequence[str],
    from_date: str,
    to_date: str,
) -> None:
    """Emit the Prefect event for EOD price coverage gaps after ingestion."""
    emit_prefect_event(
        event=PrefectEvent.COVERAGE_GATE_FAILED,
        resource_id=f"unique-stocks.coverage-gate.{app_run_id}",
        resource_name="EOD coverage gate",
        labels={
            "unique-stocks.resource.kind": "coverage-gate",
            "unique-stocks.app_run_id": app_run_id,
            "unique-stocks.domain": "eod_price",
            "unique-stocks.status": "failed",
            "unique-stocks.severity": "warning",
        },
        related=[_app_run_related_resource(app_run_id)],
        payload={
            "app_run_id": app_run_id,
            "gaps_count": gaps_count,
            "provider_exchange_codes": list(provider_exchange_codes),
            "from_date": from_date,
            "to_date": to_date,
        },
    )


def emit_prefect_ingestion_status_event(
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
    event = PrefectEvent.INGESTION_PARTIAL if status == "partial" else PrefectEvent.INGESTION_FAILED
    emit_prefect_event(
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
        related=[_app_run_related_resource(app_run_id, flow_name=flow_name, domain=domain)],
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


def emit_prefect_stale_runs_event(
    *,
    stale_runs: Sequence[dict[str, Any]],
    older_than_minutes: int,
) -> None:
    """Emit the Prefect audit event for pipeline runs stuck in running state."""
    emit_prefect_event(
        event=PrefectEvent.PIPELINE_STALE_RUNNING,
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


def emit_prefect_pipeline_cancelled_event(
    *,
    app_run_id: str,
    flow_name: str,
    error_class: str,
    error_message: str,
) -> None:
    """Emit the Prefect audit event for a pipeline run cancelled by orchestration."""
    emit_prefect_event(
        event=PrefectEvent.PIPELINE_CANCELLED,
        resource_id=f"unique-stocks.pipeline-run.{app_run_id}",
        resource_name=flow_name,
        labels={
            "unique-stocks.resource.kind": "pipeline-run",
            "unique-stocks.app_run_id": app_run_id,
            "unique-stocks.flow_name": flow_name,
            "unique-stocks.status": "cancelled",
            "unique-stocks.severity": "warning",
        },
        related=[_app_run_related_resource(app_run_id, flow_name=flow_name)],
        payload={
            "app_run_id": app_run_id,
            "flow_name": flow_name,
            "error_class": error_class,
            "error_message": error_message,
        },
    )


async def publish_prefect_ingestion_summary(
    *,
    flow_name: str,
    domain: str,
    app_run_id: str,
    status: str,
    summary: dict[str, Any],
) -> None:
    """Create a Prefect summary artifact and emit a partial/failed event if needed."""
    artifact_key = await _create_ingestion_summary_artifact(
        flow_name=flow_name,
        domain=domain,
        app_run_id=app_run_id,
        status=status,
        summary=summary,
    )
    emit_prefect_ingestion_status_event(
        flow_name=flow_name,
        domain=domain,
        app_run_id=app_run_id,
        status=status,
        summary=summary,
        artifact_key=artifact_key,
    )


async def _create_ingestion_summary_artifact(
    *,
    flow_name: str,
    domain: str,
    app_run_id: str,
    status: str,
    summary: dict[str, Any],
) -> str | None:
    artifact_key = f"ingestion-{_slug(flow_name)}-{_slug(app_run_id)[:16]}"
    try:
        get_run_context()
    except RuntimeError:
        logger.debug(
            "prefect_ingestion_summary_artifact_skipped",
            flow_name=flow_name,
            domain=domain,
            run_id=app_run_id,
            reason="missing_run_context",
        )
        return None

    try:
        summary_json = json.dumps(_jsonable(summary), default=str, indent=2, sort_keys=True)
        if len(summary_json) > 12_000:
            summary_json = f"{summary_json[:12_000]}\n... truncated ..."
        body = "\n".join(
            [
                f"# {flow_name} {status}",
                "",
                f"- Domain: `{domain}`",
                f"- App run: `{app_run_id}`",
                f"- Status: `{status}`",
                "",
                "```json",
                summary_json,
                "```",
            ]
        )
        artifact_id = create_markdown_artifact(
            key=artifact_key,
            markdown=body,
            description=f"{flow_name} ingestion summary ({status}).",
        )
        if inspect.isawaitable(artifact_id):
            await artifact_id
        return artifact_key
    except Exception as exc:
        logger.warning(
            "prefect_ingestion_summary_artifact_failed",
            flow_name=flow_name,
            domain=domain,
            run_id=app_run_id,
            error=str(exc),
        )
        return None


def _base_resource_labels(labels: EventLabels | None) -> dict[str, str]:
    return _string_labels(
        {
            "unique-stocks.app": APP_LABEL,
            "unique-stocks.service": PIPELINES_LABEL,
            "unique-stocks.environment": os.getenv("ENVIRONMENT", "dev"),
            **dict(labels or {}),
        }
    )


def _related_resources(related: Sequence[EventRelatedResource] | None) -> list[dict[str, str]]:
    resources = [_string_labels(resource) for resource in related or ()]
    resources.extend(_current_prefect_related_resources())
    return _dedupe_related_resources([resource for resource in resources if resource])


def _current_prefect_related_resources() -> list[dict[str, str]]:
    resources: list[dict[str, str]] = []
    flow_run_context = FlowRunContext.get()
    task_run_context = TaskRunContext.get()
    flow_run = getattr(flow_run_context, "flow_run", None) if flow_run_context else None
    task_run = getattr(task_run_context, "task_run", None) if task_run_context else None

    flow_run_id = _object_value(flow_run, "id") or _object_value(task_run, "flow_run_id")
    if flow_run_id:
        resources.append(
            _string_labels(
                {
                    "prefect.resource.id": f"prefect.flow-run.{flow_run_id}",
                    "prefect.resource.name": _object_value(flow_run, "name"),
                    "prefect.resource.role": "flow-run",
                }
            )
        )

    if task_run_id := _object_value(task_run, "id"):
        task = getattr(task_run_context, "task", None) if task_run_context else None
        resources.append(
            _string_labels(
                {
                    "prefect.resource.id": f"prefect.task-run.{task_run_id}",
                    "prefect.resource.name": _object_value(task, "name") or _object_value(task_run, "name"),
                    "prefect.resource.role": "task-run",
                }
            )
        )

    if flow_id := _object_value(flow_run, "flow_id"):
        flow = getattr(flow_run_context, "flow", None) if flow_run_context else None
        resources.append(
            _string_labels(
                {
                    "prefect.resource.id": f"prefect.flow.{flow_id}",
                    "prefect.resource.name": _object_value(flow, "name"),
                    "prefect.resource.role": "flow",
                }
            )
        )

    if deployment_id := _object_value(flow_run, "deployment_id"):
        resources.append(
            _string_labels(
                {
                    "prefect.resource.id": f"prefect.deployment.{deployment_id}",
                    "prefect.resource.name": _object_value(flow_run, "deployment_name"),
                    "prefect.resource.role": "deployment",
                }
            )
        )

    if work_queue_id := _object_value(flow_run, "work_queue_id"):
        resources.append(
            _string_labels(
                {
                    "prefect.resource.id": f"prefect.work-queue.{work_queue_id}",
                    "prefect.resource.name": _object_value(flow_run, "work_queue_name"),
                    "prefect.resource.role": "work-queue",
                }
            )
        )

    if work_pool_name := _object_value(flow_run, "work_pool_name"):
        resources.append(
            _string_labels(
                {
                    "prefect.resource.id": f"prefect.work-pool.{work_pool_name}",
                    "prefect.resource.name": work_pool_name,
                    "prefect.resource.role": "work-pool",
                }
            )
        )

    for tag in _object_sequence(flow_run, "tags"):
        resources.append(
            _string_labels(
                {
                    "prefect.resource.id": f"prefect.tag.{tag}",
                    "prefect.resource.name": tag,
                    "prefect.resource.role": "tag",
                }
            )
        )

    return resources


def _app_run_related_resource(
    app_run_id: str | None,
    *,
    flow_name: str | None = None,
    domain: str | None = None,
) -> dict[str, str]:
    return _string_labels(
        {
            "prefect.resource.id": f"unique-stocks.pipeline-run.{app_run_id}" if app_run_id else None,
            "prefect.resource.name": flow_name or app_run_id,
            "prefect.resource.role": "app-run",
            "unique-stocks.app_run_id": app_run_id,
            "unique-stocks.flow_name": flow_name,
            "unique-stocks.domain": domain,
        }
    )


def _object_value(source: object | None, name: str) -> str | None:
    if source is None:
        return None
    try:
        value = getattr(source, name, None)
    except Exception:
        return None
    return str(value) if value else None


def _object_sequence(source: object | None, name: str) -> tuple[str, ...]:
    if source is None:
        return ()
    try:
        value = getattr(source, name, None)
    except Exception:
        return ()
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray | memoryview):
        return tuple(str(item) for item in value if item)
    return ()


def _dedupe_related_resources(resources: Sequence[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str | None, str | None]] = set()
    deduped: list[dict[str, str]] = []
    for resource in resources:
        identity = (resource.get("prefect.resource.id"), resource.get("prefect.resource.role"))
        if identity in seen:
            continue
        seen.add(identity)
        deduped.append(resource)
    return deduped


def _string_labels(labels: EventLabels) -> dict[str, str]:
    cleaned: dict[str, str] = {}
    for key, value in labels.items():
        if value is None:
            continue
        normalized = str(value).strip()
        if not normalized:
            continue
        cleaned[key] = normalized
    return cleaned


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


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9-]+", "-", value).strip("-").lower()
    return slug or "run"


def _jsonable(value: Any) -> Any:
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray | memoryview):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date | datetime | UUID):
        return str(value)
    return value
