"""Tests for production-facing operational health helpers."""

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from core.infrastructure.health import operational as operational_health


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
        self.last_query = ""

    def table_exists(self, schema: str, table: str) -> bool:
        """Return whether the fake exposes pipeline.runs."""
        return self.has_runs_table and (schema, table) == ("pipeline", "runs")

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Return stale running rows."""
        _ = sql, params
        return self.stale_rows

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Return the configured latest row for a requested domain."""
        self.last_query = sql
        domain = str((params or [""])[0])
        return self.recent_rows.get(domain)

    def close(self) -> None:
        """Capture close calls."""
        self.closed = True


def test_stale_running_runs_reports_missing_audit_table() -> None:
    """Missing audit tables should fail operational health."""
    lake = FakeOperationalLake(has_runs_table=False)

    rows = operational_health.stale_running_runs(lake, older_than=datetime.now(UTC))

    assert rows == [{"flow_name": "pipeline.runs", "started_at": None, "status": "missing_table"}]


def test_recent_domain_runs_requires_each_configured_domain() -> None:
    """Each requested domain should have a recent completed run."""
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

    rows = operational_health.recent_domain_runs(
        lake,
        domains=["eod_price", "fundamental"],
        since=datetime.now(UTC) - timedelta(hours=36),
    )

    assert rows["eod_price"] is recent["eod_price"]
    assert rows["fundamental"] is None


def test_recent_domain_runs_requires_completed_status() -> None:
    """Partial and skipped runs should not satisfy production freshness."""
    lake = FakeOperationalLake(recent_rows={"eod_price": None})

    operational_health.recent_domain_runs(
        lake,
        domains=["eod_price"],
        since=datetime.now(UTC) - timedelta(hours=36),
    )

    assert "status = 'completed'" in lake.last_query
    assert "partial" not in lake.last_query
    assert "skipped" not in lake.last_query


def test_run_operational_health_emits_stale_runs_event() -> None:
    """Stale running rows should become a Prefect event for automations."""
    stale_rows = [{"run_id": "run-1", "flow_name": "flow", "domain": "eod_price"}]
    events: list[dict[str, object]] = []

    class StaleLake(FakeOperationalLake):
        """Lake double with one stale row."""

        def __init__(self, *_: object, **__: object) -> None:
            super().__init__(stale_rows=stale_rows)

    def emit_stale(**kwargs: object) -> None:
        events.append(kwargs)

    result = operational_health.run_operational_health(
        operational_health.OperationalHealthConfig(
            prefect_api_url="http://prefect.example/api",
            stale_running_hours=1.5,
            recent_domains=[],
            recent_hours=36.0,
            lake_read_only=False,
        ),
        lake_factory=StaleLake,
        api_health_check=lambda *_args, **_kwargs: True,
        stale_runs_event=emit_stale,
        now=datetime(2026, 6, 13, 12, 0, tzinfo=UTC),
    )

    assert result.failures == ["1 stale running pipeline run(s)"]
    assert events == [{"stale_runs": stale_rows, "older_than_minutes": 90}]


def test_run_operational_health_reports_redacted_lake_error_detail() -> None:
    """Operational alerts should include useful lake errors without leaking tokens."""

    class FailingLake:
        """Lake double that fails on construction like a connection error."""

        def __init__(self, *_: object, **__: object) -> None:
            """Raise a connection-style error containing a sensitive query param."""
            raise RuntimeError("connect failed for md:unique_stocks?motherduck_token=secret-token")

        def table_exists(self, schema: str, table: str) -> bool:
            """Return whether a table exists."""
            _ = schema, table
            return False

        def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
            """Return rows."""
            _ = sql, params
            return []

        def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
            """Return one row."""
            _ = sql, params
            return None

        def close(self) -> None:
            """Close the fake lake."""

    result = operational_health.run_operational_health(
        operational_health.OperationalHealthConfig(
            prefect_api_url="http://prefect.example/api",
            stale_running_hours=6.0,
            recent_domains=[],
            recent_hours=36.0,
            lake_read_only=False,
        ),
        lake_factory=FailingLake,
        api_health_check=lambda *_args, **_kwargs: True,
    )

    assert len(result.failures) == 1
    assert "RuntimeError: connect failed" in result.failures[0]
    assert "secret-token" not in result.failures[0]
    assert "motherduck_token=[redacted]" in result.failures[0]
