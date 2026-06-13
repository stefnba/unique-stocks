"""Tests for production-facing operational health helpers."""

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from core.operations import operational_health


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


def test_configured_recent_domains_uses_cli_values(monkeypatch: Any) -> None:
    """CLI domains should override monitor environment defaults."""
    monkeypatch.setenv("OPERATIONAL_HEALTH_RECENT_DOMAINS", "fundamental")

    domains = operational_health.configured_recent_domains([" eod_price ", ""])

    assert domains == ["eod_price"]


def test_configured_recent_domains_uses_environment(monkeypatch: Any) -> None:
    """The deployed healthcheck can configure freshness domains through env vars."""
    monkeypatch.setenv("OPERATIONAL_HEALTH_RECENT_DOMAINS", "eod_price, fundamental exchange")

    domains = operational_health.configured_recent_domains(None)

    assert domains == ["eod_price", "fundamental", "exchange"]


def test_operational_lake_read_only_defaults_to_false_for_motherduck_token(monkeypatch: Any) -> None:
    """Regular MotherDuck tokens cannot be opened with DuckDB read_only=True."""
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "token")
    monkeypatch.delenv("OPERATIONAL_HEALTH_LAKE_READ_ONLY", raising=False)

    assert operational_health.operational_lake_read_only() is False


def test_operational_lake_read_only_can_be_forced_for_read_scaling_token(monkeypatch: Any) -> None:
    """Operators with a read-scaling token can force a read-only MotherDuck connection."""
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "token")
    monkeypatch.setenv("OPERATIONAL_HEALTH_LAKE_READ_ONLY", "true")

    assert operational_health.operational_lake_read_only() is True


def test_main_emits_stale_runs_event(monkeypatch: Any, capsys: Any) -> None:
    """Stale running rows should become a Prefect event for automations."""
    stale_rows = [{"run_id": "run-1", "flow_name": "flow", "domain": "eod_price"}]
    events: list[dict[str, object]] = []

    class StaleLake(FakeOperationalLake):
        """Lake double with one stale row."""

        def __init__(self, *_: object, **__: object) -> None:
            super().__init__(stale_rows=stale_rows)

    def emit_stale(**kwargs: object) -> None:
        events.append(kwargs)

    monkeypatch.setenv("PREFECT_API_URL", "http://prefect.example/api")
    monkeypatch.setattr(operational_health, "prefect_api_is_healthy", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(operational_health, "DataLakeClient", StaleLake)
    monkeypatch.setattr(operational_health, "emit_prefect_stale_runs_event", emit_stale)

    exit_code = operational_health.main(["--stale-running-hours", "1.5"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "1 stale running pipeline run(s)" in captured.err
    assert events == [{"stale_runs": stale_rows, "older_than_minutes": 90}]


def test_main_reports_redacted_lake_error_detail(monkeypatch: Any, capsys: Any) -> None:
    """Operational alerts should include useful lake errors without leaking tokens."""

    class FailingLake:
        """Lake double that fails on construction like a connection error."""

        def __init__(self, *_: object, **__: object) -> None:
            """Raise a connection-style error containing a sensitive query param."""
            raise RuntimeError("connect failed for md:unique_stocks?motherduck_token=secret-token")

    monkeypatch.setenv("PREFECT_API_URL", "http://prefect.example/api")
    monkeypatch.setattr(operational_health, "prefect_api_is_healthy", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(operational_health, "DataLakeClient", FailingLake)

    exit_code = operational_health.main([])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "RuntimeError: connect failed" in captured.err
    assert "secret-token" not in captured.err
    assert "motherduck_token=[redacted]" in captured.err
