"""Aggregate run-status helpers for pipeline audit rows."""

from core.ingestion.run_tracking.records import RunStatus


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
