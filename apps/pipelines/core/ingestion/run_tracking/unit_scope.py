"""Context-managed audit scope for one pipeline work unit."""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.ingestion.landing import LandingWrite
from core.ingestion.run_tracking.records import UnitStatus

if TYPE_CHECKING:
    from core.ingestion.run_tracking.run_scope import PipelineRunScope


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
