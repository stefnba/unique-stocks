"""Context-managed audit scope for one pipeline run."""

from __future__ import annotations

from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from core.ingestion.landing import LandingWrite
from core.ingestion.run_tracking.records import (
    LandingObjectRecord,
    RejectionRecord,
    RunCounters,
    RunStatus,
    RunUnitRecord,
    RunUnitTally,
    UnitStatus,
)
from core.ingestion.run_tracking.unit_scope import PipelineUnitScope
from core.ingestion.run_tracking.utils import _landing_values

if TYPE_CHECKING:
    from core.ingestion.run_tracking.writer import PipelineRunTracker


class PipelineRunScope:
    """Context-managed pipeline run that prevents accidental orphan rows.

    Attributes:
        tracker: Low-level writer used by the scope.
        run_id: Active ``pipeline.runs`` id.
        domain: Bound domain applied to units, landing objects, and rejections.
        provider: Default provider applied to child audit records.
        tally: Count of successfully recorded work-unit outcomes.
    """

    def __init__(
        self,
        tracker: PipelineRunTracker,
        run_id: str,
        *,
        flow_name: str,
        domain: str,
        provider: str | None,
    ) -> None:
        """Create a scope for an already-started run.

        Args:
            tracker: Audit writer that owns the lake connection.
            run_id: Existing ``pipeline.runs`` id.
            flow_name: Prefect/logical flow name.
            domain: Domain bound to child audit rows.
            provider: Default provider bound to child audit rows.
        """
        self.tracker = tracker
        self.run_id = run_id
        self.flow_name = flow_name
        self.domain = domain
        self.provider = provider
        self.tally = RunUnitTally()
        self._terminal = False

    @property
    def is_terminal(self) -> bool:
        """Return True when the scope already wrote a terminal state."""
        return self._terminal

    def complete(
        self,
        *,
        status: RunStatus = "completed",
        counters: RunCounters | None = None,
        rows_raw: int | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
        summary: dict[str, object] | None = None,
    ) -> None:
        """Mark this scoped run complete.

        Args:
            status: Terminal run status to write.
            counters: Explicit counters. When omitted, the scope tally provides unit counts.
            rows_raw: Optional raw row count used when ``counters`` is omitted.
            rows_valid: Optional valid row count used when ``counters`` is omitted.
            rows_rejected: Optional rejected row count used when ``counters`` is omitted.
            rows_written: Optional written row count used when ``counters`` is omitted.
            summary: Optional compact JSON summary for dashboard/debug use.
        """
        self.tracker.complete_run(
            self.run_id,
            status=status,
            counters=counters
            or self.tally.counters(
                rows_raw=rows_raw,
                rows_valid=rows_valid,
                rows_rejected=rows_rejected,
                rows_written=rows_written,
            ),
            summary=summary,
        )
        self._terminal = True

    def cancel(
        self,
        error: BaseException | str,
        *,
        counters: RunCounters | None = None,
        rows_raw: int | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
        summary: dict[str, object] | None = None,
    ) -> None:
        """Mark this scoped run cancelled."""
        self.tracker.cancel_run(
            self.run_id,
            error,
            flow_name=self.flow_name,
            counters=counters
            or self.tally.counters(
                rows_raw=rows_raw,
                rows_valid=rows_valid,
                rows_rejected=rows_rejected,
                rows_written=rows_written,
            ),
            summary=summary,
        )
        self._terminal = True

    def fail(
        self,
        error: BaseException | str,
        *,
        counters: RunCounters | None = None,
        rows_raw: int | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
        summary: dict[str, object] | None = None,
    ) -> None:
        """Mark this scoped run failed.

        Args:
            error: Exception or message that explains the run failure.
            counters: Explicit counters. When omitted, the scope tally provides unit counts.
            rows_raw: Optional raw row count used when ``counters`` is omitted.
            rows_valid: Optional valid row count used when ``counters`` is omitted.
            rows_rejected: Optional rejected row count used when ``counters`` is omitted.
            rows_written: Optional written row count used when ``counters`` is omitted.
            summary: Optional compact JSON summary for dashboard/debug use.
        """
        self.tracker.fail_run(
            self.run_id,
            error,
            counters=counters
            or self.tally.counters(
                rows_raw=rows_raw,
                rows_valid=rows_valid,
                rows_rejected=rows_rejected,
                rows_written=rows_written,
            ),
            summary=summary,
        )
        self._terminal = True

    def record_unit(
        self,
        *,
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
        """Record one unit using this run's domain/provider and update the tally.

        Args:
            unit_type: Domain-defined work-unit grain.
            unit_key: JSON-serializable natural key for the unit.
            status: Terminal unit status.
            provider: Optional provider override for this unit.
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
                self.unit_record(
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

    def unit_record(
        self,
        *,
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
    ) -> RunUnitRecord:
        """Build a unit record with this run's domain/provider for batched inserts.

        Args:
            unit_type: Domain-defined work-unit grain.
            unit_key: JSON-serializable natural key for the unit.
            status: Terminal unit status.
            provider: Optional provider override for this unit.
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
            A prebuilt unit record for ``record_units``.
        """
        return RunUnitRecord(
            run_id=self.run_id,
            unit_id=unit_id,
            domain=self.domain,
            provider=self.provider if provider is None else provider,
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
        )

    def record_units(self, records: Sequence[RunUnitRecord]) -> list[str]:
        """Record prebuilt unit rows and update this run's tally.

        Args:
            records: Unit records already bound to the run.

        Returns:
            Inserted unit ids, in the same order as ``records``.
        """
        unit_ids = self.tracker.record_units(records)
        self.tally.extend(records)
        return unit_ids

    def record_unit_with_landing(
        self,
        landing: LandingWrite,
        *,
        unit_type: str,
        unit_key: dict[str, object],
        status: UnitStatus = "completed",
        provider: str | None = None,
        reason: str | None = None,
        rows_raw: int | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        error: BaseException | str | None = None,
        unit_id: str | UUID | None = None,
    ) -> str:
        """Record one unit and its landing object using this run's bound context.

        Args:
            landing: Metadata returned by the landing-zone write task.
            unit_type: Domain-defined work-unit grain.
            unit_key: JSON-serializable natural key for the unit.
            status: Terminal unit status.
            provider: Optional provider override for this unit and landing object.
            reason: Optional domain reason for the outcome.
            rows_raw: Raw row count override. Defaults from ``landing.rows_raw``.
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
        recorded_unit_id = self.record_unit(
            provider=provider,
            unit_type=unit_type,
            unit_key=unit_key,
            status=status,
            reason=reason,
            source_uri=landing.source_uri,
            rows_raw=landing.rows_raw if rows_raw is None else rows_raw,
            rows_valid=rows_valid,
            rows_rejected=rows_rejected,
            rows_written=rows_written,
            started_at=started_at,
            completed_at=completed_at,
            error=error,
            unit_id=unit_id,
        )
        self.record_landing_object(landing, unit_id=recorded_unit_id, provider=provider)
        return recorded_unit_id

    def record_landing_object(
        self,
        landing: LandingWrite | None = None,
        *,
        dataset: str | None = None,
        source_uri: str | None = None,
        unit_id: str | UUID | None = None,
        provider: str | None = None,
        partition: dict[str, object] | None = None,
        rows_raw: int | None = None,
        byte_count: int | None = None,
        content_hash: str | None = None,
    ) -> None:
        """Record one landing object using this run's domain/provider.

        Args:
            landing: Optional ``LandingWrite`` metadata returned by a landing task.
            dataset: Audit dataset override when ``landing`` is not supplied.
            source_uri: Landing object URI override when ``landing`` is not supplied.
            unit_id: Optional linked work-unit id.
            provider: Optional provider override.
            partition: Optional landing partition values.
            rows_raw: Optional raw row count override.
            byte_count: Optional object byte size.
            content_hash: Optional object content hash.
        """
        self.record_landing_objects(
            [
                self.landing_object_record(
                    landing,
                    dataset=dataset,
                    source_uri=source_uri,
                    unit_id=unit_id,
                    provider=provider,
                    partition=partition,
                    rows_raw=rows_raw,
                    byte_count=byte_count,
                    content_hash=content_hash,
                )
            ]
        )

    def landing_object_record(
        self,
        landing: LandingWrite | None = None,
        *,
        dataset: str | None = None,
        source_uri: str | None = None,
        unit_id: str | UUID | None = None,
        provider: str | None = None,
        partition: Mapping[str, object] | None = None,
        rows_raw: int | None = None,
        byte_count: int | None = None,
        content_hash: str | None = None,
    ) -> LandingObjectRecord:
        """Build a landing-object record with this run's domain/provider for batched inserts.

        Args:
            landing: Optional ``LandingWrite`` metadata returned by a landing task.
            dataset: Audit dataset override when ``landing`` is not supplied.
            source_uri: Landing object URI override when ``landing`` is not supplied.
            unit_id: Optional linked work-unit id.
            provider: Optional provider override.
            partition: Optional landing partition values.
            rows_raw: Optional raw row count override.
            byte_count: Optional object byte size.
            content_hash: Optional object content hash.

        Returns:
            A prebuilt landing object record for ``record_landing_objects``.
        """
        dataset, source_uri, partition, rows_raw, byte_count, content_hash = _landing_values(
            landing,
            dataset=dataset,
            source_uri=source_uri,
            partition=partition,
            rows_raw=rows_raw,
            byte_count=byte_count,
            content_hash=content_hash,
        )
        return LandingObjectRecord(
            run_id=self.run_id,
            unit_id=unit_id,
            domain=self.domain,
            provider=self.provider if provider is None else provider,
            dataset=dataset,
            source_uri=source_uri,
            partition=partition,
            rows_raw=rows_raw,
            byte_count=byte_count,
            content_hash=content_hash,
        )

    def record_landing_objects(self, records: Sequence[LandingObjectRecord]) -> None:
        """Record prebuilt landing-object rows.

        Args:
            records: Landing-object records already bound to the run.
        """
        self.tracker.record_landing_objects(records)

    def rejection_record(
        self,
        *,
        raw_fragment: object,
        reason: str,
        unit_id: str | UUID | None = None,
        entity_key: dict[str, object] | None = None,
        source_uri: str | None = None,
        error: BaseException | str | None = None,
    ) -> RejectionRecord:
        """Build a rejection record with this run's bound context.

        Args:
            raw_fragment: Raw row or payload fragment to sample.
            reason: Domain rejection reason.
            unit_id: Optional linked work-unit id.
            entity_key: Entity key used for debugging and hash uniqueness.
            source_uri: Optional landing object URI.
            error: Optional parser/domain error.

        Returns:
            A prebuilt rejection record for ``record_rejections``.
        """
        return RejectionRecord(
            run_id=self.run_id,
            unit_id=unit_id,
            domain=self.domain,
            entity_key=entity_key,
            source_uri=source_uri,
            raw_fragment=raw_fragment,
            reason=reason,
            error=error,
        )

    def record_rejection(
        self,
        *,
        raw_fragment: object,
        reason: str,
        unit_id: str | UUID | None = None,
        entity_key: dict[str, object] | None = None,
        source_uri: str | None = None,
        error: BaseException | str | None = None,
    ) -> None:
        """Record one parser rejection using this run's domain.

        Args:
            raw_fragment: Raw row or payload fragment to sample.
            reason: Domain rejection reason.
            unit_id: Optional linked work-unit id.
            entity_key: Entity key used for debugging and hash uniqueness.
            source_uri: Optional landing object URI.
            error: Optional parser/domain error.
        """
        self.record_rejections(
            [
                self.rejection_record(
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
        """Record prebuilt parser rejection rows.

        Args:
            records: Rejection records already bound to the run.
            limit: Optional cap for high-volume rejection sampling.

        Returns:
            Number of rejection rows inserted.
        """
        return self.tracker.record_rejections(records, limit=limit)

    @contextmanager
    def track_unit(
        self,
        *,
        unit_type: str,
        unit_key: dict[str, object],
        provider: str | None = None,
    ) -> Generator[PipelineUnitScope]:
        """Track one unit and record it failed if the unit scope raises.

        Args:
            unit_type: Domain-defined work-unit grain.
            unit_key: JSON-serializable natural key for the unit.
            provider: Optional provider override for this unit.

        Yields:
            A unit scope that must be completed, skipped, unsupported, or failed.
        """
        scope = PipelineUnitScope(
            self,
            unit_type=unit_type,
            unit_key=unit_key,
            provider=provider,
        )
        try:
            yield scope
        except BaseException as exc:
            if not scope.is_terminal:
                scope.fail(exc, rows_written=0)
            raise
        else:
            if not scope.is_terminal:
                message = "Unit scope exited without a terminal state."
                scope.fail(message, rows_written=0)
                raise RuntimeError(message)
