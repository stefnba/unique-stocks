"""Tests for production-facing operational health helpers."""

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from scripts import check_operational_health


class FakeOperationalLake:
    """Minimal fake lake for operational health helper tests."""

    def __init__(
        self,
        *,
        has_runs_table: bool = True,
        stale_rows: list[dict[str, Any]] | None = None,
        recent_rows: dict[str, dict[str, Any] | None] | None = None,
    ) -> None:
        """Configure fake audit responses."""
        self.has_runs_table = has_runs_table
        self.stale_rows = stale_rows or []
        self.recent_rows = recent_rows or {}
        self.closed = False

    def table_exists(self, schema: str, table: str) -> bool:
        """Return whether the fake exposes pipeline.runs."""
        return self.has_runs_table and (schema, table) == ("pipeline", "runs")

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Return stale running rows."""
        _ = sql, params
        return self.stale_rows

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Return the configured latest row for a requested domain."""
        _ = sql
        domain = str((params or [""])[0])
        return self.recent_rows.get(domain)

    def close(self) -> None:
        """Capture close calls."""
        self.closed = True


def test_stale_running_runs_reports_missing_audit_table() -> None:
    """Missing audit tables should fail operational health."""
    lake = FakeOperationalLake(has_runs_table=False)

    rows = check_operational_health.stale_running_runs(lake, older_than=datetime.now(UTC))

    assert rows == [{"flow_name": "pipeline.runs", "started_at": None, "status": "missing_table"}]


def test_recent_domain_runs_requires_each_configured_domain() -> None:
    """Each requested domain should have a recent terminal run."""
    recent: dict[str, dict[str, Any] | None] = {
        "eod_price": {
            "run_id": "018f0000-0000-7000-8000-000000000001",
            "flow_name": "eod-price-daily",
            "domain": "eod_price",
            "status": "completed",
            "completed_at": datetime.now(UTC),
        }
    }
    lake = FakeOperationalLake(recent_rows=recent)

    rows = check_operational_health.recent_domain_runs(
        lake,
        domains=["eod_price", "fundamental"],
        since=datetime.now(UTC) - timedelta(hours=36),
    )

    assert rows["eod_price"] is recent["eod_price"]
    assert rows["fundamental"] is None
