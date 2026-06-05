"""Tests for fundamentals ingestion batch date resolution."""

from datetime import date

from domains.fundamental.batch import resolve_fundamental_snapshot_date

PINNED = date(2026, 6, 1)
LEGACY = date(2026, 6, 2)
BRONZE_LATEST = date(2026, 5, 15)
TODAY = date(2026, 6, 10)


def test_resolve_prefers_ingestion_batch_date() -> None:
    """Explicit ingestion_batch_date wins over snapshot_date and bronze latest."""
    resolved = resolve_fundamental_snapshot_date(
        snapshot_date=LEGACY,
        ingestion_batch_date=PINNED,
        continue_ingestion_batch=True,
        latest_bronze_snapshot_date=BRONZE_LATEST,
        default_date=TODAY,
    )
    assert resolved.snapshot_date == PINNED
    assert resolved.source == "ingestion_batch_date"


def test_resolve_continues_latest_bronze_batch() -> None:
    """continue_ingestion_batch reuses bronze MAX when no explicit date is passed."""
    resolved = resolve_fundamental_snapshot_date(
        snapshot_date=None,
        ingestion_batch_date=None,
        continue_ingestion_batch=True,
        latest_bronze_snapshot_date=BRONZE_LATEST,
        default_date=TODAY,
    )
    assert resolved.snapshot_date == BRONZE_LATEST
    assert resolved.source == "bronze_latest"


def test_resolve_defaults_to_today_without_continue() -> None:
    """Manual-style runs without continue still open today's partition."""
    resolved = resolve_fundamental_snapshot_date(
        snapshot_date=None,
        ingestion_batch_date=None,
        continue_ingestion_batch=False,
        latest_bronze_snapshot_date=BRONZE_LATEST,
        default_date=TODAY,
    )
    assert resolved.snapshot_date == TODAY
    assert resolved.source == "today"
