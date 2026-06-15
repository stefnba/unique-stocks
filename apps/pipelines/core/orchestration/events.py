"""Orchestration event vocabulary and Prefect-backed publishing helpers."""

from __future__ import annotations

import inspect
import json
import re
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

import structlog
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event

logger = structlog.get_logger(__name__)


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
) -> None:
    """Emit one JSON-safe Prefect event for pipeline automations.

    Event publication is an observability side effect: failures are logged but
    do not fail the caller's ingestion or transformation work.
    """
    try:
        emit_event(
            event=event,
            resource={
                "prefect.resource.id": resource_id,
                "prefect.resource.name": resource_name,
            },
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
) -> None:
    """Emit a Prefect alert event for terminal partial or failed ingestion."""
    if status not in {"partial", "failed"}:
        return
    emit_prefect_event(
        event=PrefectEvent.INGESTION_PARTIAL if status == "partial" else PrefectEvent.INGESTION_FAILED,
        resource_id=f"unique-stocks.ingestion-run.{app_run_id}",
        resource_name=flow_name,
        payload={
            "app_run_id": app_run_id,
            "flow_name": flow_name,
            "domain": domain,
            "status": status,
            "summary": summary,
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
        payload={
            "older_than_minutes": older_than_minutes,
            "stale_run_count": len(stale_runs),
            "stale_runs": list(stale_runs),
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
    await _create_ingestion_summary_artifact(
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
    )


async def _create_ingestion_summary_artifact(
    *,
    flow_name: str,
    domain: str,
    app_run_id: str,
    status: str,
    summary: dict[str, Any],
) -> None:
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
            key=f"ingestion-{_slug(flow_name)}-{_slug(app_run_id)[:16]}",
            markdown=body,
            description=f"{flow_name} ingestion summary ({status}).",
        )
        if inspect.isawaitable(artifact_id):
            await artifact_id
    except Exception as exc:
        logger.warning(
            "prefect_ingestion_summary_artifact_failed",
            flow_name=flow_name,
            domain=domain,
            run_id=app_run_id,
            error=str(exc),
        )


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
