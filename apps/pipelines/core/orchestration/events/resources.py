"""Prefect resource builders for app-owned events."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence

from prefect.context import FlowRunContext, TaskRunContext

from core.orchestration.events.contracts import APP_LABEL, PIPELINES_LABEL

type EventLabels = Mapping[str, object | None]
type EventRelatedResource = Mapping[str, object | None]


def event_resource(
    *,
    resource_id: str,
    resource_name: str,
    labels: EventLabels | None = None,
) -> dict[str, str]:
    """Build the primary Prefect event resource with automation match labels."""
    return _string_labels(
        {
            "prefect.resource.id": resource_id,
            "prefect.resource.name": resource_name,
            "unique-stocks.app": APP_LABEL,
            "unique-stocks.service": PIPELINES_LABEL,
            "unique-stocks.environment": os.getenv("ENVIRONMENT", "dev"),
            **dict(labels or {}),
        }
    )


def related_resources(related: Sequence[EventRelatedResource] | None = None) -> list[dict[str, str]]:
    """Combine explicit related resources with the active Prefect run context."""
    resources = [_string_labels(resource) for resource in related or ()]
    resources.extend(current_prefect_related_resources())
    return _dedupe_related_resources([resource for resource in resources if resource])


def app_run_related_resource(
    app_run_id: str | None,
    *,
    flow_name: str | None = None,
    domain: str | None = None,
) -> dict[str, str]:
    """Represent an app-level pipeline run as a related Prefect resource."""
    if not app_run_id:
        return {}
    return _related_resource(
        resource_id=f"unique-stocks.pipeline-run.{app_run_id}",
        role="app-run",
        name=flow_name or app_run_id,
        labels={
            "unique-stocks.app_run_id": app_run_id,
            "unique-stocks.flow_name": flow_name,
            "unique-stocks.domain": domain,
        },
    )


def current_prefect_related_resources() -> list[dict[str, str]]:
    """Return related Prefect runtime resources without making Prefect API calls."""
    resources: list[dict[str, str]] = []
    flow_run_context = FlowRunContext.get()
    task_run_context = TaskRunContext.get()
    flow_run = getattr(flow_run_context, "flow_run", None) if flow_run_context else None
    task_run = getattr(task_run_context, "task_run", None) if task_run_context else None

    flow_run_id = _object_value(flow_run, "id") or _object_value(task_run, "flow_run_id")
    resources.append(
        _related_resource(
            resource_id=f"prefect.flow-run.{flow_run_id}" if flow_run_id else None,
            role="flow-run",
            name=_object_value(flow_run, "name"),
        )
    )

    if task_run_id := _object_value(task_run, "id"):
        task = getattr(task_run_context, "task", None) if task_run_context else None
        resources.append(
            _related_resource(
                resource_id=f"prefect.task-run.{task_run_id}",
                role="task-run",
                name=_object_value(task, "name") or _object_value(task_run, "name"),
            )
        )

    if flow_id := _object_value(flow_run, "flow_id"):
        flow = getattr(flow_run_context, "flow", None) if flow_run_context else None
        resources.append(
            _related_resource(
                resource_id=f"prefect.flow.{flow_id}",
                role="flow",
                name=_object_value(flow, "name"),
            )
        )

    if deployment_id := _object_value(flow_run, "deployment_id"):
        resources.append(
            _related_resource(
                resource_id=f"prefect.deployment.{deployment_id}",
                role="deployment",
                name=_object_value(flow_run, "deployment_name"),
            )
        )

    if work_queue_id := _object_value(flow_run, "work_queue_id"):
        resources.append(
            _related_resource(
                resource_id=f"prefect.work-queue.{work_queue_id}",
                role="work-queue",
                name=_object_value(flow_run, "work_queue_name"),
            )
        )

    if work_pool_name := _object_value(flow_run, "work_pool_name"):
        resources.append(
            _related_resource(
                resource_id=f"prefect.work-pool.{work_pool_name}",
                role="work-pool",
                name=work_pool_name,
            )
        )

    for tag in _object_sequence(flow_run, "tags"):
        resources.append(
            _related_resource(
                resource_id=f"prefect.tag.{tag}",
                role="tag",
                name=tag,
            )
        )

    return [resource for resource in resources if resource]


def _related_resource(
    *,
    resource_id: str | None,
    role: str,
    name: object | None = None,
    labels: EventLabels | None = None,
) -> dict[str, str]:
    if not resource_id:
        return {}
    return _string_labels(
        {
            "prefect.resource.id": resource_id,
            "prefect.resource.name": name,
            "prefect.resource.role": role,
            **dict(labels or {}),
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


__all__ = [
    "EventLabels",
    "EventRelatedResource",
    "app_run_related_resource",
    "current_prefect_related_resources",
    "event_resource",
    "related_resources",
]
