"""Low-level lake writer for pipeline audit facts.

This module owns SQL/lake writes for ``pipeline.*`` audit tables. Domain flows
usually enter through ``PipelineRunTracker.track_run()`` and use the scope
objects from ``scopes.py`` for day-to-day ergonomics.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from hashlib import sha256
from uuid import UUID

import structlog

from core.ingestion.run_tracking.records import (
    LandingObjectRecord,
    RejectionRecord,
    RunCounters,
    RunStatus,
    RunUnitRecord,
    UnitStatus,
)
from core.ingestion.run_tracking.scopes import PipelineRunScope
from core.ingestion.run_tracking.utils import (
    _ERROR_LIMIT,
    _code_version,
    _current_prefect_flow_run_id,
    _is_cancelled_exception,
    _json_dict,
    _json_hash,
    _now,
    _raw_sample,
    _uuid_or_none,
)
from core.ingestion.serialization import canonical_json, jsonable
from core.lake import DataLakeClient, get_lake_client
from core.orchestration.events import emit_pipeline_cancelled

log = structlog.get_logger(__name__)


class PipelineRunTracker:
    """Low-level writer for pipeline audit facts.

    Most domain services should prefer ``track_run()`` and the returned
    ``PipelineRunScope``. The direct methods remain available for tests,
    compatibility, and rare batch paths that need explicit row construction.
    """

    def __init__(self, lake: DataLakeClient | None = None) -> None:
        """Create a tracker backed by the configured lake client.

        Args:
            lake: Optional lake client override, mainly for tests.
        """
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
        """Insert a ``running`` row in ``pipeline.runs``.

        Args:
            flow_name: Prefect/logical flow name.
            domain: Logical pipeline domain.
            run_kind: Domain run kind, such as ``daily`` or ``snapshot``.
            provider: Optional source provider identifier.
            parameters: JSON-serializable run parameters.
            target_window_start: Optional logical data-window start.
            target_window_end: Optional logical data-window end.
            parent_run_id: Optional parent run id when chaining known runs.

        Returns:
            Newly generated run id.
        """
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
        log.info(
            "pipeline.run.started",
            run_id=run_id,
            flow_name=flow_name,
            domain=domain,
            run_kind=run_kind,
            provider=provider,
            target_window_start=target_window_start,
            target_window_end=target_window_end,
        )
        return run_id

    @contextmanager
    def track_run(
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
    ) -> Generator[PipelineRunScope]:
        """Start a run and guard it against non-terminal exits.

        Args:
            flow_name: Prefect/logical flow name.
            domain: Logical pipeline domain.
            run_kind: Domain run kind, such as ``daily`` or ``snapshot``.
            provider: Optional source provider identifier.
            parameters: JSON-serializable run parameters.
            target_window_start: Optional logical data-window start.
            target_window_end: Optional logical data-window end.
            parent_run_id: Optional parent run id when chaining known runs.

        Yields:
            A run scope that must be explicitly completed or failed.

        Raises:
            RuntimeError: If the scope exits without a terminal state.
        """
        run_id = self.start_run(
            flow_name=flow_name,
            domain=domain,
            run_kind=run_kind,
            provider=provider,
            parameters=parameters,
            target_window_start=target_window_start,
            target_window_end=target_window_end,
            parent_run_id=parent_run_id,
        )
        context_tokens = structlog.contextvars.bind_contextvars(
            run_id=run_id,
            flow_name=flow_name,
            domain=domain,
            run_kind=run_kind,
            provider=provider,
        )
        scope = PipelineRunScope(
            self,
            run_id,
            flow_name=flow_name,
            domain=domain,
            provider=provider,
        )
        try:
            yield scope
        except BaseException as exc:
            if not scope.is_terminal:
                if _is_cancelled_exception(exc):
                    scope.cancel(exc)
                else:
                    scope.fail(exc)
            raise
        else:
            if not scope.is_terminal:
                message = "Run scope exited without a terminal state."
                scope.fail(message)
                raise RuntimeError(message)
        finally:
            structlog.contextvars.reset_contextvars(**context_tokens)

    def complete_run(
        self,
        run_id: str | UUID,
        *,
        status: RunStatus = "completed",
        counters: RunCounters | None = None,
        summary: dict[str, object] | None = None,
    ) -> None:
        """Mark a pipeline run terminal with optional aggregate counters.

        Args:
            run_id: Run id to update.
            status: Terminal run status.
            counters: Optional aggregate counters.
            summary: Optional compact JSON summary.
        """
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
        log.info(
            "pipeline.run.completed",
            run_id=str(run_id),
            status=status,
            units_total=counters.units_total,
            units_succeeded=counters.units_succeeded,
            units_failed=counters.units_failed,
            units_skipped=counters.units_skipped,
            rows_raw=counters.rows_raw,
            rows_valid=counters.rows_valid,
            rows_rejected=counters.rows_rejected,
            rows_written=counters.rows_written,
        )

    def fail_run(
        self,
        run_id: str | UUID,
        error: BaseException | str,
        *,
        counters: RunCounters | None = None,
        summary: dict[str, object] | None = None,
    ) -> None:
        """Mark a pipeline run failed with a compact terminal error.

        Args:
            run_id: Run id to update.
            error: Exception or message that explains the failure.
            counters: Optional aggregate counters.
            summary: Optional compact JSON summary.
        """
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
        log.error(
            "pipeline.run.failed",
            run_id=str(run_id),
            error_class=error_class,
            error_message=error_message,
            units_total=counters.units_total,
            units_succeeded=counters.units_succeeded,
            units_failed=counters.units_failed,
            units_skipped=counters.units_skipped,
            rows_raw=counters.rows_raw,
            rows_valid=counters.rows_valid,
            rows_rejected=counters.rows_rejected,
            rows_written=counters.rows_written,
        )

    def cancel_run(
        self,
        run_id: str | UUID,
        error: BaseException | str,
        *,
        flow_name: str,
        counters: RunCounters | None = None,
        summary: dict[str, object] | None = None,
    ) -> None:
        """Mark a pipeline run cancelled with a compact terminal reason."""
        counters = counters or RunCounters()
        error_class = type(error).__name__ if isinstance(error, BaseException) else None
        error_message = str(error)[:_ERROR_LIMIT]
        self.lake.execute(
            """
            UPDATE pipeline.runs
               SET status = 'cancelled',
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
        emit_pipeline_cancelled(
            app_run_id=str(run_id),
            flow_name=flow_name,
            error_class=error_class or "Cancelled",
            error_message=error_message,
        )
        log.warning(
            "pipeline.run.cancelled",
            run_id=str(run_id),
            error_class=error_class,
            error_message=error_message,
            units_total=counters.units_total,
            units_succeeded=counters.units_succeeded,
            units_failed=counters.units_failed,
            units_skipped=counters.units_skipped,
            rows_raw=counters.rows_raw,
            rows_valid=counters.rows_valid,
            rows_rejected=counters.rows_rejected,
            rows_written=counters.rows_written,
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
        unit_id: str | UUID | None = None,
    ) -> str:
        """Insert one work-unit outcome.

        Args:
            run_id: Parent run id.
            domain: Logical pipeline domain.
            unit_type: Domain-defined work-unit grain.
            unit_key: JSON-serializable natural key for the unit.
            status: Terminal unit status.
            provider: Optional source provider identifier.
            reason: Optional domain reason for the outcome.
            source_uri: Optional landing object URI linked to the unit.
            rows_raw: Raw rows observed for the unit.
            rows_valid: Valid parsed rows for the unit.
            rows_rejected: Rejected rows for the unit.
            rows_written: Rows written for the unit when known at this grain.
            started_at: Optional unit start time.
            completed_at: Optional unit completion time.
            error: Optional failure error/message.
            unit_id: Optional caller-provided unit id.

        Returns:
            Inserted unit id.
        """
        return self.record_units(
            [
                RunUnitRecord(
                    run_id=run_id,
                    domain=domain,
                    provider=provider,
                    unit_type=unit_type,
                    unit_key=unit_key,
                    status=status,
                    reason=reason,
                    source_uri=source_uri,
                    rows_raw=rows_raw,
                    rows_valid=rows_valid,
                    rows_rejected=rows_rejected,
                    rows_written=rows_written,
                    started_at=started_at,
                    completed_at=completed_at,
                    error=error,
                    unit_id=unit_id,
                )
            ]
        )[0]

    def record_units(self, records: Sequence[RunUnitRecord]) -> list[str]:
        """Insert work-unit outcomes in one lake write.

        Args:
            records: Unit records to insert.

        Returns:
            Inserted unit ids, in the same order as ``records``.
        """
        if not records:
            return []
        rows = []
        unit_ids = []
        now = _now()
        for record in records:
            unit_id = str(record.unit_id or uuid.uuid4())
            unit_ids.append(unit_id)
            normalized_key = _json_dict(record.unit_key)
            error_class = type(record.error).__name__ if isinstance(record.error, BaseException) else None
            error_message = str(record.error)[:_ERROR_LIMIT] if record.error is not None else None
            rows.append(
                {
                    "unit_id": unit_id,
                    "run_id": str(record.run_id),
                    "domain": record.domain,
                    "provider": record.provider,
                    "unit_type": record.unit_type,
                    "unit_key_hash": _json_hash(normalized_key),
                    "unit_key_json": normalized_key,
                    "status": record.status,
                    "reason": record.reason,
                    "source_uri": record.source_uri,
                    "rows_raw": record.rows_raw,
                    "rows_valid": record.rows_valid,
                    "rows_rejected": record.rows_rejected,
                    "rows_written": record.rows_written,
                    "started_at": record.started_at,
                    "completed_at": record.completed_at or now,
                    "error_class": error_class,
                    "error_message": error_message,
                }
            )
        self.lake.insert_rows(
            "pipeline",
            "run_units",
            rows,
        )
        for row in rows:
            if row["status"] == "failed":
                log.warning(
                    "pipeline.unit.failed",
                    run_id=row["run_id"],
                    unit_id=row["unit_id"],
                    domain=row["domain"],
                    provider=row["provider"],
                    unit_type=row["unit_type"],
                    unit_key_hash=row["unit_key_hash"],
                    error_class=row["error_class"],
                    error_message=row["error_message"],
                )
        return unit_ids

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
        """Record one raw landing object used by a pipeline unit.

        Args:
            run_id: Parent run id.
            domain: Logical pipeline domain.
            dataset: Audit dataset label.
            source_uri: S3/object-store URI.
            unit_id: Optional linked work-unit id.
            provider: Optional source provider identifier.
            partition: Optional landing partition values.
            rows_raw: Raw rows in the object when known.
            byte_count: Object byte size when known.
            content_hash: Object content hash when known.
        """
        self.record_landing_objects(
            [
                LandingObjectRecord(
                    run_id=run_id,
                    domain=domain,
                    provider=provider,
                    dataset=dataset,
                    source_uri=source_uri,
                    unit_id=unit_id,
                    partition=partition,
                    rows_raw=rows_raw,
                    byte_count=byte_count,
                    content_hash=content_hash,
                )
            ]
        )

    def record_landing_objects(self, records: Sequence[LandingObjectRecord]) -> None:
        """Insert landing-object audit records in one lake write.

        Args:
            records: Landing object records to insert.
        """
        if not records:
            return
        now = _now()
        self.lake.insert_rows(
            "pipeline",
            "landing_objects",
            [
                {
                    "run_id": str(record.run_id),
                    "unit_id": _uuid_or_none(record.unit_id),
                    "domain": record.domain,
                    "provider": record.provider,
                    "dataset": record.dataset,
                    "source_uri": record.source_uri,
                    "partition_json": _json_dict(record.partition) if record.partition is not None else None,
                    "rows_raw": record.rows_raw,
                    "byte_count": record.byte_count,
                    "content_hash": record.content_hash,
                    "recorded_at": now,
                }
                for record in records
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
        """Record one structured parser rejection.

        Args:
            run_id: Parent run id.
            domain: Logical pipeline domain.
            raw_fragment: Raw row or payload fragment to sample.
            reason: Domain rejection reason.
            unit_id: Optional linked work-unit id.
            entity_key: Entity key used for debugging and hash uniqueness.
            source_uri: Optional landing object URI.
            error: Optional parser/domain error.
        """
        self.record_rejections(
            [
                RejectionRecord(
                    run_id=run_id,
                    domain=domain,
                    raw_fragment=raw_fragment,
                    reason=reason,
                    unit_id=unit_id,
                    entity_key=entity_key,
                    source_uri=source_uri,
                    error=error,
                )
            ]
        )

    def record_rejections(self, records: Sequence[RejectionRecord], *, limit: int | None = None) -> int:
        """Insert parser rejection audit records in one lake write.

        Args:
            records: Rejections to write.
            limit: Optional cap for sampling high-volume rejection bursts.

        Returns:
            Number of rejection rows inserted.
        """
        if limit is not None:
            records = records[: max(0, limit)]
        if not records:
            return 0
        now = _now()
        rows = []
        for record in records:
            normalized_raw = jsonable(record.raw_fragment)
            normalized_entity_key = _json_dict(record.entity_key) if record.entity_key is not None else None
            error_class = type(record.error).__name__ if isinstance(record.error, BaseException) else None
            error_message = str(record.error)[:_ERROR_LIMIT] if record.error is not None else None
            rows.append(
                {
                    "run_id": str(record.run_id),
                    "unit_id": _uuid_or_none(record.unit_id),
                    "domain": record.domain,
                    "entity_key_json": normalized_entity_key,
                    "source_uri": record.source_uri,
                    "raw_hash": sha256(
                        canonical_json({"entity_key": normalized_entity_key, "raw": normalized_raw}).encode()
                    ).hexdigest(),
                    "reason": record.reason,
                    "error_class": error_class,
                    "error_message": error_message,
                    "raw_sample_json": _raw_sample(normalized_raw),
                    "recorded_at": now,
                }
            )
        self.lake.insert_rows(
            "pipeline",
            "rejections",
            rows,
        )
        return len(rows)

    def stale_running_runs(self, *, older_than_hours: int = 2) -> list[dict[str, object]]:
        """Return running runs older than the given threshold.

        Args:
            older_than_hours: Minimum run age in hours.

        Returns:
            Raw lake rows for matching stale ``running`` runs.
        """
        return self.lake.query(
            """
            SELECT *
              FROM pipeline.runs
             WHERE status = 'running'
               AND started_at < now() - (? * INTERVAL '1 hour')
             ORDER BY started_at
            """,
            [older_than_hours],
        )
