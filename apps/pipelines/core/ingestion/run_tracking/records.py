"""Record types for pipeline run tracking."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

type RunStatus = Literal["running", "completed", "partial", "failed", "skipped", "cancelled"]
type UnitStatus = Literal["completed", "failed", "skipped", "unsupported"]


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


__all__ = [
    "LandingObjectRecord",
    "RejectionRecord",
    "RunCounters",
    "RunStatus",
    "RunUnitRecord",
    "RunUnitTally",
    "UnitStatus",
]
