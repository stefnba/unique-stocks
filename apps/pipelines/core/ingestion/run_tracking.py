"""Data-plane audit helpers for pipeline runs.

Prefect remains the orchestration control plane. This module records durable
domain facts in the lake: what work was attempted, what was skipped or failed,
what landed, and how many rows moved through each stage.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from core.clients.lake import DataLakeClient, get_lake_client
from core.ingestion.serialization import canonical_json, jsonable

type RunStatus = Literal["running", "completed", "partial", "failed", "skipped", "cancelled"]
type UnitStatus = Literal["completed", "failed", "skipped", "unsupported"]

_ERROR_LIMIT = 2000


@dataclass(frozen=True, slots=True)
class RunCounters:
    """Aggregate run counters stored on ``pipeline.runs``."""

    units_total: int | None = None
    units_succeeded: int | None = None
    units_failed: int | None = None
    units_skipped: int | None = None
    rows_raw: int | None = None
    rows_valid: int | None = None
    rows_rejected: int | None = None
    rows_written: int | None = None


class PipelineRunTracker:
    """Write audit facts for one pipeline flow run."""

    def __init__(self, lake: DataLakeClient | None = None) -> None:
        """Create a tracker backed by the configured lake client."""
        self.lake = lake or get_lake_client()

    def start_run(
        self,
        *,
        flow_name: str,
        domain: str,
        run_kind: str,
        provider: str | None = None,
        parameters: dict[str, object] | None = None,
        target_window_start: date | None = None,
        target_window_end: date | None = None,
        parent_run_id: str | UUID | None = None,
    ) -> str:
        """Insert a ``running`` row in ``pipeline.runs`` and return its id."""
        run_id = str(uuid.uuid4())
        self.lake.insert_rows(
            "pipeline",
            "runs",
            [
                {
                    "run_id": run_id,
                    "parent_run_id": _uuid_or_none(parent_run_id),
                    "prefect_flow_run_id": _current_prefect_flow_run_id(),
                    "flow_name": flow_name,
                    "domain": domain,
                    "run_kind": run_kind,
                    "provider": provider,
                    "environment": os.getenv("ENVIRONMENT"),
                    "code_version": _code_version(),
                    "parameters_json": jsonable(parameters) if parameters is not None else None,
                    "target_window_start": target_window_start,
                    "target_window_end": target_window_end,
                    "status": "running",
                    "started_at": _now(),
                }
            ],
        )
        return run_id

    def complete_run(
        self,
        run_id: str | UUID,
        *,
        status: RunStatus = "completed",
        counters: RunCounters | None = None,
        summary: dict[str, object] | None = None,
    ) -> None:
        """Mark a pipeline run terminal with optional aggregate counters."""
        counters = counters or RunCounters()
        self.lake.execute(
            """
            UPDATE pipeline.runs
               SET status = ?,
                   completed_at = ?,
                   units_total = ?,
                   units_succeeded = ?,
                   units_failed = ?,
                   units_skipped = ?,
                   rows_raw = ?,
                   rows_valid = ?,
                   rows_rejected = ?,
                   rows_written = ?,
                   summary_json = ?
             WHERE run_id = ?
            """,
            [
                status,
                _now(),
                counters.units_total,
                counters.units_succeeded,
                counters.units_failed,
                counters.units_skipped,
                counters.rows_raw,
                counters.rows_valid,
                counters.rows_rejected,
                counters.rows_written,
                canonical_json(summary) if summary is not None else None,
                str(run_id),
            ],
        )

    def fail_run(
        self,
        run_id: str | UUID,
        error: BaseException | str,
        *,
        counters: RunCounters | None = None,
        summary: dict[str, object] | None = None,
    ) -> None:
        """Mark a pipeline run failed with a compact terminal error."""
        counters = counters or RunCounters()
        error_class = type(error).__name__ if isinstance(error, BaseException) else None
        error_message = str(error)[:_ERROR_LIMIT]
        self.lake.execute(
            """
            UPDATE pipeline.runs
               SET status = 'failed',
                   completed_at = ?,
                   units_total = ?,
                   units_succeeded = ?,
                   units_failed = ?,
                   units_skipped = ?,
                   rows_raw = ?,
                   rows_valid = ?,
                   rows_rejected = ?,
                   rows_written = ?,
                   summary_json = ?,
                   error_class = ?,
                   error_message = ?
             WHERE run_id = ?
            """,
            [
                _now(),
                counters.units_total,
                counters.units_succeeded,
                counters.units_failed,
                counters.units_skipped,
                counters.rows_raw,
                counters.rows_valid,
                counters.rows_rejected,
                counters.rows_written,
                canonical_json(summary) if summary is not None else None,
                error_class,
                error_message,
                str(run_id),
            ],
        )

    def record_unit(
        self,
        *,
        run_id: str | UUID,
        domain: str,
        unit_type: str,
        unit_key: dict[str, object],
        status: UnitStatus,
        provider: str | None = None,
        reason: str | None = None,
        source_uri: str | None = None,
        rows_raw: int | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        error: BaseException | str | None = None,
    ) -> str:
        """Insert one work-unit outcome and return its id."""
        unit_id = str(uuid.uuid4())
        normalized_key = _json_dict(unit_key)
        error_class = type(error).__name__ if isinstance(error, BaseException) else None
        error_message = str(error)[:_ERROR_LIMIT] if error is not None else None
        self.lake.insert_rows(
            "pipeline",
            "run_units",
            [
                {
                    "unit_id": unit_id,
                    "run_id": str(run_id),
                    "domain": domain,
                    "provider": provider,
                    "unit_type": unit_type,
                    "unit_key_hash": _json_hash(normalized_key),
                    "unit_key_json": normalized_key,
                    "status": status,
                    "reason": reason,
                    "source_uri": source_uri,
                    "rows_raw": rows_raw,
                    "rows_valid": rows_valid,
                    "rows_rejected": rows_rejected,
                    "rows_written": rows_written,
                    "started_at": started_at,
                    "completed_at": completed_at or _now(),
                    "error_class": error_class,
                    "error_message": error_message,
                }
            ],
        )
        return unit_id

    def record_landing_object(
        self,
        *,
        run_id: str | UUID,
        domain: str,
        dataset: str,
        source_uri: str,
        unit_id: str | UUID | None = None,
        provider: str | None = None,
        partition: dict[str, object] | None = None,
        rows_raw: int | None = None,
        byte_count: int | None = None,
        content_hash: str | None = None,
    ) -> None:
        """Record one raw landing object used by a pipeline unit."""
        self.lake.insert_rows(
            "pipeline",
            "landing_objects",
            [
                {
                    "run_id": str(run_id),
                    "unit_id": _uuid_or_none(unit_id),
                    "domain": domain,
                    "provider": provider,
                    "dataset": dataset,
                    "source_uri": source_uri,
                    "partition_json": _json_dict(partition) if partition is not None else None,
                    "rows_raw": rows_raw,
                    "byte_count": byte_count,
                    "content_hash": content_hash,
                    "recorded_at": _now(),
                }
            ],
        )

    def record_rejection(
        self,
        *,
        run_id: str | UUID,
        domain: str,
        raw_fragment: object,
        reason: str,
        unit_id: str | UUID | None = None,
        entity_key: dict[str, object] | None = None,
        source_uri: str | None = None,
        error: BaseException | str | None = None,
    ) -> None:
        """Record one structured parser rejection."""
        normalized_raw = jsonable(raw_fragment)
        error_class = type(error).__name__ if isinstance(error, BaseException) else None
        error_message = str(error)[:_ERROR_LIMIT] if error is not None else None
        self.lake.insert_rows(
            "pipeline",
            "rejections",
            [
                {
                    "run_id": str(run_id),
                    "unit_id": _uuid_or_none(unit_id),
                    "domain": domain,
                    "entity_key_json": _json_dict(entity_key) if entity_key is not None else None,
                    "source_uri": source_uri,
                    "raw_hash": sha256(canonical_json(normalized_raw).encode()).hexdigest(),
                    "reason": reason,
                    "error_class": error_class,
                    "error_message": error_message,
                    "raw_sample_json": _raw_sample(normalized_raw),
                    "recorded_at": _now(),
                }
            ],
        )


def terminal_status(*, failed: int = 0, rejected: int = 0, skipped_all: bool = False) -> RunStatus:
    """Return the aggregate run status from unit counters."""
    if failed:
        return "partial"
    if skipped_all:
        return "skipped"
    if rejected:
        return "partial"
    return "completed"


def _current_prefect_flow_run_id() -> str | None:
    """Return the active Prefect flow run id when running inside Prefect."""
    try:
        from prefect.runtime import flow_run
    except ImportError:
        return None
    try:
        value = getattr(flow_run, "id", None)
    except Exception:
        return None
    return str(value) if value else None


def _code_version() -> str | None:
    """Return a configured source revision if the deployment exposes one."""
    for name in ("GIT_SHA", "SOURCE_COMMIT", "COMMIT_SHA", "IMAGE_TAG"):
        if value := os.getenv(name):
            return value
    return None


def _json_hash(value: dict[str, object]) -> str:
    return sha256(canonical_json(value).encode()).hexdigest()


def _json_dict(value: dict[str, object]) -> dict[str, object]:
    normalized = jsonable(value)
    if not isinstance(normalized, dict):
        raise TypeError("Expected JSON-normalized mapping")
    return normalized


def _raw_sample(value: object) -> dict[str, object] | None:
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return {"items": value[:10]}
    if value is None:
        return None
    return {"value": value}


def _uuid_or_none(value: str | UUID | None) -> str | None:
    return str(value) if value is not None else None


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


__all__ = [
    "PipelineRunTracker",
    "RunCounters",
    "RunStatus",
    "UnitStatus",
    "terminal_status",
]
