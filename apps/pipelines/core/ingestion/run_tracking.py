"""Data-plane audit helpers for pipeline runs.

Prefect remains the orchestration control plane. This module records durable
domain facts in the lake: what work was attempted, what was skipped or failed,
what landed, and how many rows moved through each stage.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from core.clients.lake import DataLakeClient, get_lake_client
from core.ingestion.landing import LandingWrite
from core.ingestion.serialization import canonical_json, jsonable

type RunStatus = Literal["running", "completed", "partial", "failed", "skipped", "cancelled"]
type UnitStatus = Literal["completed", "failed", "skipped", "unsupported"]

_ERROR_LIMIT = 2000


@dataclass(frozen=True, slots=True)
class RunCounters:
    """Aggregate counters written to ``pipeline.runs``.

    Attributes:
        units_total: Number of recorded ``pipeline.run_units`` rows for the run.
        units_succeeded: Number of recorded work units with ``completed`` status.
        units_failed: Number of recorded work units with ``failed`` status.
        units_skipped: Number of recorded work units with ``skipped`` or ``unsupported`` status.
        rows_raw: Provider/raw rows observed by the run.
        rows_valid: Parsed rows that passed validation.
        rows_rejected: Rows rejected by parser or domain validation.
        rows_written: Rows written to Bronze or the relevant downstream table.
    """

    units_total: int | None = None
    units_succeeded: int | None = None
    units_failed: int | None = None
    units_skipped: int | None = None
    rows_raw: int | None = None
    rows_valid: int | None = None
    rows_rejected: int | None = None
    rows_written: int | None = None


@dataclass(slots=True)
class RunUnitTally:
    """Mutable aggregate of successfully recorded work-unit statuses.

    The tally is owned by ``PipelineRunScope`` and advances only after unit
    audit rows have been written, so run counters match ``pipeline.run_units``.

    Attributes:
        total: Number of recorded units.
        succeeded: Number of completed units.
        failed: Number of failed units.
        skipped: Number of skipped or unsupported units.
    """

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0

    def record(self, status: UnitStatus) -> None:
        """Add one recorded work unit to the tally.

        Args:
            status: Terminal unit status that was successfully written.
        """
        self.total += 1
        if status == "completed":
            self.succeeded += 1
        elif status == "failed":
            self.failed += 1
        elif status in {"skipped", "unsupported"}:
            self.skipped += 1

    def extend(self, records: Sequence[RunUnitRecord]) -> None:
        """Add several recorded work units to the tally.

        Args:
            records: Unit records that have just been inserted.
        """
        for record in records:
            self.record(record.status)

    def counters(
        self,
        *,
        rows_raw: int | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
    ) -> RunCounters:
        """Return run counters using this tally for unit counts.

        Args:
            rows_raw: Optional aggregate raw row count.
            rows_valid: Optional aggregate valid row count.
            rows_rejected: Optional aggregate rejected row count.
            rows_written: Optional aggregate written row count.

        Returns:
            A ``RunCounters`` object suitable for completing/failing a run.
        """
        return RunCounters(
            units_total=self.total,
            units_succeeded=self.succeeded,
            units_failed=self.failed,
            units_skipped=self.skipped,
            rows_raw=rows_raw,
            rows_valid=rows_valid,
            rows_rejected=rows_rejected,
            rows_written=rows_written,
        )


@dataclass(frozen=True, slots=True)
class RunUnitRecord:
    """One work-unit audit record pending insert.

    Attributes:
        run_id: Parent ``pipeline.runs.run_id``.
        domain: Logical domain, such as ``eod_price`` or ``instrument``.
        unit_type: Domain-defined unit grain, such as ``exchange_date`` or ``ticker_backfill``.
        unit_key: JSON-serializable natural key for replay/debugging.
        status: Terminal unit status.
        provider: Optional source provider identifier.
        reason: Optional domain reason, such as ``already_ingested`` or ``no_data``.
        source_uri: Optional raw landing object URI associated with the unit.
        rows_raw: Raw rows observed for the unit.
        rows_valid: Valid parsed rows for the unit.
        rows_rejected: Rejected rows for the unit.
        rows_written: Rows written for the unit when known at this grain.
        started_at: Optional unit start time.
        completed_at: Optional unit completion time; defaults at insert time.
        error: Optional compact failure error.
        unit_id: Optional caller-provided id for linking prebuilt records.
    """

    run_id: str | UUID
    domain: str
    unit_type: str
    unit_key: dict[str, object]
    status: UnitStatus
    provider: str | None = None
    reason: str | None = None
    source_uri: str | None = None
    rows_raw: int | None = None
    rows_valid: int | None = None
    rows_rejected: int | None = None
    rows_written: int | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: BaseException | str | None = None
    unit_id: str | UUID | None = None


@dataclass(frozen=True, slots=True)
class LandingObjectRecord:
    """One landing-object audit record pending insert.

    Attributes:
        run_id: Parent ``pipeline.runs.run_id``.
        domain: Logical domain that produced or consumed the landing object.
        dataset: Audit dataset label, usually inferred as ``<domain>.<landing_field>``.
        source_uri: S3/object-store URI of the landed raw payload.
        unit_id: Optional linked work-unit id.
        provider: Optional source provider identifier.
        partition: JSON-serializable landing partition values.
        rows_raw: Number of raw records in the object when known.
        byte_count: Object byte size when available.
        content_hash: Object content hash when available.
    """

    run_id: str | UUID
    domain: str
    dataset: str
    source_uri: str
    unit_id: str | UUID | None = None
    provider: str | None = None
    partition: dict[str, object] | None = None
    rows_raw: int | None = None
    byte_count: int | None = None
    content_hash: str | None = None


@dataclass(frozen=True, slots=True)
class RejectionRecord:
    """One parser rejection audit record pending insert.

    Attributes:
        run_id: Parent ``pipeline.runs.run_id``.
        domain: Logical domain whose parser rejected the row.
        raw_fragment: JSON-serializable raw row or payload fragment.
        reason: Domain rejection reason.
        unit_id: Optional linked work-unit id.
        entity_key: JSON key identifying the rejected entity, included in ``raw_hash``.
        source_uri: Optional source landing object URI.
        error: Optional parser/domain error associated with the rejection.
    """

    run_id: str | UUID
    domain: str
    raw_fragment: object
    reason: str
    unit_id: str | UUID | None = None
    entity_key: dict[str, object] | None = None
    source_uri: str | None = None
    error: BaseException | str | None = None


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
        domain: str,
        provider: str | None,
    ) -> None:
        """Create a scope for an already-started run.

        Args:
            tracker: Audit writer that owns the lake connection.
            run_id: Existing ``pipeline.runs`` id.
            domain: Domain bound to child audit rows.
            provider: Default provider bound to child audit rows.
        """
        self.tracker = tracker
        self.run_id = run_id
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


class PipelineUnitScope:
    """Context-managed audit helper for one unit inside a run.

    Attributes:
        run: Parent run scope.
        unit_type: Domain-defined work-unit grain.
        unit_key: JSON-serializable natural key for this unit.
        provider: Optional provider override for this unit.
        unit_id: Inserted unit id after a terminal unit method is called.
    """

    def __init__(
        self,
        run: PipelineRunScope,
        *,
        unit_type: str,
        unit_key: dict[str, object],
        provider: str | None = None,
    ) -> None:
        """Create a unit scope bound to a run.

        Args:
            run: Parent run scope.
            unit_type: Domain-defined work-unit grain.
            unit_key: JSON-serializable natural key for this unit.
            provider: Optional provider override for this unit.
        """
        self.run = run
        self.unit_type = unit_type
        self.unit_key = unit_key
        self.provider = provider
        self.unit_id: str | None = None
        self._terminal = False

    @property
    def is_terminal(self) -> bool:
        """Return True when this unit already has a recorded outcome."""
        return self._terminal

    def complete(
        self,
        *,
        landing: LandingWrite | None = None,
        reason: str | None = None,
        source_uri: str | None = None,
        rows_raw: int | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
    ) -> str:
        """Record this unit as completed.

        Args:
            landing: Optional landing metadata whose URI/row count should populate the unit.
            reason: Optional domain reason, commonly a Bronze write reason.
            source_uri: Optional source URI override.
            rows_raw: Raw row count override. Defaults from ``landing.rows_raw``.
            rows_valid: Valid parsed rows for the unit.
            rows_rejected: Rejected rows for the unit.
            rows_written: Rows written for the unit when known at this grain.

        Returns:
            Inserted unit id.
        """
        if landing is not None:
            source_uri = source_uri or landing.source_uri
            rows_raw = landing.rows_raw if rows_raw is None else rows_raw
        return self._record(
            status="completed",
            reason=reason,
            source_uri=source_uri,
            rows_raw=rows_raw,
            rows_valid=rows_valid,
            rows_rejected=rows_rejected,
            rows_written=rows_written,
        )

    def complete_with_landing(
        self,
        landing: LandingWrite,
        *,
        reason: str | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
    ) -> str:
        """Record this unit as completed and record its landing object.

        Args:
            landing: Landing metadata returned by the landing-zone write task.
            reason: Optional domain reason, commonly a Bronze write reason.
            rows_valid: Valid parsed rows for the unit.
            rows_rejected: Rejected rows for the unit.
            rows_written: Rows written for the unit when known at this grain.

        Returns:
            Inserted unit id.
        """
        unit_id = self.complete(
            landing=landing,
            reason=reason,
            rows_valid=rows_valid,
            rows_rejected=rows_rejected,
            rows_written=rows_written,
        )
        self.record_landing_object(landing)
        return unit_id

    def fail(
        self,
        error: BaseException | str,
        *,
        rows_raw: int | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
    ) -> str:
        """Record this unit as failed.

        Args:
            error: Exception or message that explains the unit failure.
            rows_raw: Raw rows observed before failure.
            rows_valid: Valid parsed rows observed before failure.
            rows_rejected: Rejected rows observed before failure.
            rows_written: Rows written before failure.

        Returns:
            Inserted unit id.
        """
        return self._record(
            status="failed",
            rows_raw=rows_raw,
            rows_valid=rows_valid,
            rows_rejected=rows_rejected,
            rows_written=rows_written,
            error=error,
        )

    def skip(
        self,
        *,
        reason: str,
        rows_raw: int | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
    ) -> str:
        """Record this unit as skipped.

        Args:
            reason: Domain skip reason, such as ``already_ingested`` or ``no_data``.
            rows_raw: Raw rows observed before skip.
            rows_valid: Valid parsed rows observed before skip.
            rows_rejected: Rejected rows observed before skip.
            rows_written: Rows written before skip, usually zero.

        Returns:
            Inserted unit id.
        """
        return self._record(
            status="skipped",
            reason=reason,
            rows_raw=rows_raw,
            rows_valid=rows_valid,
            rows_rejected=rows_rejected,
            rows_written=rows_written,
        )

    def unsupported(
        self,
        *,
        reason: str,
        rows_written: int | None = None,
    ) -> str:
        """Record this unit as unsupported.

        Args:
            reason: Domain reason the requested unit is unsupported.
            rows_written: Rows written before discovering unsupported status.

        Returns:
            Inserted unit id.
        """
        return self._record(status="unsupported", reason=reason, rows_written=rows_written)

    def record_landing_object(
        self,
        landing: LandingWrite | None = None,
        *,
        dataset: str | None = None,
        source_uri: str | None = None,
        partition: dict[str, object] | None = None,
        rows_raw: int | None = None,
        byte_count: int | None = None,
        content_hash: str | None = None,
    ) -> None:
        """Record one landing object for this unit.

        Args:
            landing: Optional ``LandingWrite`` metadata returned by a landing task.
            dataset: Audit dataset override when ``landing`` is not supplied.
            source_uri: Landing object URI override when ``landing`` is not supplied.
            partition: Optional landing partition values.
            rows_raw: Optional raw row count override.
            byte_count: Optional object byte size.
            content_hash: Optional object content hash.

        Raises:
            RuntimeError: If called before the unit has a recorded terminal outcome.
        """
        if self.unit_id is None:
            raise RuntimeError("Cannot record a landing object before recording the unit outcome.")
        self.run.record_landing_object(
            landing,
            unit_id=self.unit_id,
            dataset=dataset,
            source_uri=source_uri,
            partition=partition,
            rows_raw=rows_raw,
            byte_count=byte_count,
            content_hash=content_hash,
        )

    def _record(
        self,
        *,
        status: UnitStatus,
        reason: str | None = None,
        source_uri: str | None = None,
        rows_raw: int | None = None,
        rows_valid: int | None = None,
        rows_rejected: int | None = None,
        rows_written: int | None = None,
        error: BaseException | str | None = None,
    ) -> str:
        if self._terminal:
            raise RuntimeError("Unit scope already has a terminal state.")
        self.unit_id = self.run.record_unit(
            provider=self.provider,
            unit_type=self.unit_type,
            unit_key=self.unit_key,
            status=status,
            reason=reason,
            source_uri=source_uri,
            rows_raw=rows_raw,
            rows_valid=rows_valid,
            rows_rejected=rows_rejected,
            rows_written=rows_written,
            error=error,
        )
        self._terminal = True
        return self.unit_id


class PipelineRunTracker:
    """Low-level writer for pipeline audit facts.

    Most domain flows should prefer ``track_run()`` and the returned
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
        scope = PipelineRunScope(
            self,
            self.start_run(
                flow_name=flow_name,
                domain=domain,
                run_kind=run_kind,
                provider=provider,
                parameters=parameters,
                target_window_start=target_window_start,
                target_window_end=target_window_end,
                parent_run_id=parent_run_id,
            ),
            domain=domain,
            provider=provider,
        )
        try:
            yield scope
        except BaseException as exc:
            if not scope.is_terminal:
                scope.fail(exc)
            raise
        else:
            if not scope.is_terminal:
                message = "Run scope exited without a terminal state."
                scope.fail(message)
                raise RuntimeError(message)

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


def terminal_status(*, failed: int = 0, rejected: int = 0, skipped_all: bool = False) -> RunStatus:
    """Return the aggregate run status from unit counters.

    Args:
        failed: Number of failed work units or node results.
        rejected: Number of parser/domain rejections.
        skipped_all: Whether all planned work was intentionally skipped.

    Returns:
        ``partial`` for failures/rejections, ``skipped`` for all-skipped runs,
        otherwise ``completed``.
    """
    if failed:
        return "partial"
    if skipped_all:
        return "skipped"
    if rejected:
        return "partial"
    return "completed"


def _landing_values(
    landing: LandingWrite | None,
    *,
    dataset: str | None,
    source_uri: str | None,
    partition: Mapping[str, object] | None,
    rows_raw: int | None,
    byte_count: int | None,
    content_hash: str | None,
) -> tuple[str, str, dict[str, object] | None, int | None, int | None, str | None]:
    if landing is not None:
        dataset = landing.dataset if dataset is None else dataset
        source_uri = landing.source_uri if source_uri is None else source_uri
        partition = landing.partition if partition is None and landing.partition is not None else partition
        rows_raw = landing.rows_raw if rows_raw is None else rows_raw
        byte_count = landing.byte_count if byte_count is None else byte_count
        content_hash = landing.content_hash if content_hash is None else content_hash
    if dataset is None or source_uri is None:
        raise ValueError("A landing object needs either LandingWrite metadata or dataset/source_uri arguments.")
    return dataset, source_uri, dict(partition) if partition is not None else None, rows_raw, byte_count, content_hash


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
    "LandingObjectRecord",
    "PipelineRunTracker",
    "PipelineRunScope",
    "PipelineUnitScope",
    "RejectionRecord",
    "RunCounters",
    "RunUnitTally",
    "RunUnitRecord",
    "RunStatus",
    "UnitStatus",
    "terminal_status",
]
