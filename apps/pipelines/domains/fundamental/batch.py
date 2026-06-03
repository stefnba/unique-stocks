"""Ingestion batch date resolution for fundamentals snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

SnapshotDateSource = Literal["ingestion_batch_date", "snapshot_date", "bronze_latest", "today"]


@dataclass(frozen=True, slots=True)
class ResolvedFundamentalSnapshotDate:
    """Effective ``snapshot_date`` for bronze partitions and idempotency checks.

    Attributes:
        snapshot_date: Date used for all ``(snapshot_date, ticker)`` keys in this run.
        source: How the date was chosen (for run summaries and debugging).
    """

    snapshot_date: date
    source: SnapshotDateSource


def resolve_fundamental_snapshot_date(
    *,
    snapshot_date: date | None,
    ingestion_batch_date: date | None,
    continue_ingestion_batch: bool,
    latest_bronze_snapshot_date: date | None,
    default_date: date,
) -> ResolvedFundamentalSnapshotDate:
    """Pick one ingestion batch date for the whole flow invocation.

    Resolution order:

    1. ``ingestion_batch_date`` — explicit campaign pin (backfill deployments).
    2. ``snapshot_date`` — legacy/alternate explicit pin.
    3. ``latest_bronze_snapshot_date`` when ``continue_ingestion_batch`` — resume
       multi-day backfills without re-opening every ticker under a new partition.
    4. ``default_date`` — typically today for incremental manual runs.
    """
    if ingestion_batch_date is not None:
        return ResolvedFundamentalSnapshotDate(
            snapshot_date=ingestion_batch_date,
            source="ingestion_batch_date",
        )
    if snapshot_date is not None:
        return ResolvedFundamentalSnapshotDate(snapshot_date=snapshot_date, source="snapshot_date")
    if continue_ingestion_batch and latest_bronze_snapshot_date is not None:
        return ResolvedFundamentalSnapshotDate(
            snapshot_date=latest_bronze_snapshot_date,
            source="bronze_latest",
        )
    return ResolvedFundamentalSnapshotDate(snapshot_date=default_date, source="today")
