"""Safe Prefect artifact publishing helpers."""

from __future__ import annotations

import inspect
import json
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from prefect.artifacts import create_markdown_artifact
from prefect.context import get_run_context

logger = structlog.get_logger(__name__)


async def create_ingestion_summary_artifact(
    *,
    flow_name: str,
    domain: str,
    app_run_id: str,
    status: str,
    summary: dict[str, Any],
) -> str | None:
    """Create a Prefect Markdown artifact for an ingestion run summary.

    Returns the artifact key when the artifact is created. Direct service calls
    outside a Prefect run context and Prefect API errors return ``None`` so
    artifact publishing never fails ingestion work.
    """
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


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9-]+", "-", value).strip("-").lower()
    return slug or "run"


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


__all__ = ["create_ingestion_summary_artifact"]
