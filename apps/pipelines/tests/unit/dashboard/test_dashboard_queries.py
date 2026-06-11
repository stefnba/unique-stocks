"""Tests for pipeline dashboard query helpers."""

from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from core.clients.lake import DataLakeClient
from dashboard.read_models import queries

RUN_ID_COMPLETED = "018f0000-0000-7000-8000-000000000001"
RUN_ID_FAILED = "018f0000-0000-7000-8000-000000000002"
RUN_ID_RUNNING = "018f0000-0000-7000-8000-000000000003"
DBT_RUN_ID = "018f0000-0000-7000-8000-000000000004"
UNIT_ID = "018f0000-0000-7000-8000-000000000005"
LANDING_ID = "018f0000-0000-7000-8000-000000000006"


def test_dashboard_queries_return_empty_results_when_runs_table_is_missing() -> None:
    """Dashboard summary queries should tolerate an unmigrated lake."""
    lake = DataLakeClient(connection=duckdb.connect(":memory:"))

    assert queries.pipeline_runs_available(lake) is False
    assert queries.run_units_available(lake) is False
    assert queries.landing_objects_available(lake) is False
    assert queries.load_status_summary(lake, since=datetime.now(UTC), domains=()) == {
        "total_runs": 0,
        "running_runs": 0,
        "completed_runs": 0,
        "partial_runs": 0,
        "failed_runs": 0,
        "attention_runs": 0,
        "units_failed": 0,
        "rows_written": 0,
        "rows_rejected": 0,
    }
    assert queries.load_recent_runs(lake, since=datetime.now(UTC), domains=(), statuses=()) == []
    assert queries.load_recent_run_units(lake, since=datetime.now(UTC), domains=(), statuses=()) == []
    assert queries.load_recent_landing_objects(lake, since=datetime.now(UTC), domains=()) == []
    assert queries.load_run_by_id(lake, run_id=RUN_ID_COMPLETED) is None
    assert queries.load_landing_object_by_id(lake, landing_id=LANDING_ID) is None


def test_dashboard_run_summary_queries() -> None:
    """Run-level queries should expose KPIs, recency, freshness, and stale runs."""
    now = datetime(2026, 6, 3, 12, 0, tzinfo=UTC)
    lake = _lake_with_dashboard_tables()
    _insert_runs(lake, now=now)

    summary = queries.load_status_summary(lake, since=now - timedelta(days=2), domains=("eod_price",))
    assert summary["total_runs"] == 3
    assert summary["completed_runs"] == 1
    assert summary["failed_runs"] == 1
    assert summary["running_runs"] == 1
    assert summary["attention_runs"] == 1
    assert summary["rows_written"] == 100

    stale_runs = queries.load_stale_running_runs(
        lake,
        older_than=now - timedelta(hours=2),
        domains=("eod_price",),
    )
    assert [str(row["run_id"]) for row in stale_runs] == [RUN_ID_RUNNING]

    latest = queries.load_latest_runs_by_domain(lake, domains=("eod_price",))
    assert len(latest) == 1
    assert str(latest[0]["run_id"]) == RUN_ID_FAILED

    recent_failed = queries.load_recent_runs(
        lake,
        since=now - timedelta(days=2),
        domains=("eod_price",),
        statuses=("failed",),
    )
    assert [str(row["run_id"]) for row in recent_failed] == [RUN_ID_FAILED]

    completed_run = queries.load_run_by_id(lake, run_id=RUN_ID_COMPLETED)
    assert completed_run is not None
    assert str(completed_run["run_id"]) == RUN_ID_COMPLETED
    assert completed_run["flow_name"] == "eod-price-daily"

    missing_run = queries.load_run_by_id(lake, run_id="018f0000-0000-7000-8000-000000000999")
    assert missing_run is None

    trend = queries.load_daily_run_trend(lake, since=now - timedelta(days=2), domains=("eod_price",))
    assert len(trend) == 1
    assert trend[0]["runs"] == 3

    domain_summary = queries.load_domain_run_summary(lake, since=now - timedelta(days=2), domains=("eod_price",))
    assert domain_summary == [
        {
            "domain": "eod_price",
            "runs": 3,
            "running_runs": 1,
            "completed_runs": 1,
            "attention_runs": 1,
            "units_failed": 1,
            "rows_written": 100,
            "rows_rejected": 0,
            "latest_started_at": now - timedelta(hours=1),
        }
    ]


def test_dashboard_attention_queries() -> None:
    """Triage queries should load attention runs independently of explorer filters."""
    now = datetime(2026, 6, 3, 12, 0, tzinfo=UTC)
    lake = _lake_with_dashboard_tables()
    _insert_runs(lake, now=now)

    attention_runs = queries.load_attention_runs(lake, since=now - timedelta(days=2), domains=("eod_price",))
    assert [str(row["run_id"]) for row in attention_runs] == [RUN_ID_FAILED]

    attention_by_domain = queries.load_latest_attention_runs_by_domain(
        lake,
        since=now - timedelta(days=2),
        domains=("eod_price", "instrument"),
    )
    assert len(attention_by_domain) == 1
    assert str(attention_by_domain[0]["run_id"]) == RUN_ID_FAILED
    assert attention_by_domain[0]["domain"] == "eod_price"


def test_dashboard_detail_queries() -> None:
    """Run drill-down queries should read units, landing, rejections, and dbt nodes."""
    now = datetime(2026, 6, 3, 12, 0, tzinfo=UTC)
    lake = _lake_with_dashboard_tables()
    _insert_runs(lake, now=now)
    _insert_detail_rows(lake, now=now)

    unit_status = queries.load_unit_status_breakdown(lake, run_id=RUN_ID_COMPLETED)
    assert unit_status == [{"status": "completed", "units": 1}]

    units = queries.load_run_units(lake, run_id=RUN_ID_COMPLETED)
    assert len(units) == 1
    assert units[0]["unit_key_hash"] == "hash-1"

    recent_units = queries.load_recent_run_units(
        lake,
        since=now - timedelta(days=2),
        domains=("eod_price",),
        statuses=("completed",),
    )
    assert len(recent_units) == 1
    assert str(recent_units[0]["unit_id"]) == UNIT_ID
    assert recent_units[0]["flow_name"] == "eod-price-daily"

    unit = queries.load_run_unit_by_id(lake, run_id=RUN_ID_COMPLETED, unit_id=UNIT_ID)
    assert unit is not None
    assert unit["unit_key_hash"] == "hash-1"

    missing_unit = queries.load_run_unit_by_id(lake, run_id=RUN_ID_COMPLETED, unit_id=RUN_ID_FAILED)
    assert missing_unit is None

    landing_objects = queries.load_landing_objects(lake, run_id=RUN_ID_COMPLETED)
    assert len(landing_objects) == 1
    assert landing_objects[0]["source_uri"] == "s3://bucket/object.json"

    recent_landing_objects = queries.load_recent_landing_objects(
        lake,
        since=now - timedelta(days=2),
        domains=("eod_price",),
    )
    assert len(recent_landing_objects) == 1
    assert str(recent_landing_objects[0]["landing_id"]) == LANDING_ID
    assert recent_landing_objects[0]["flow_name"] == "eod-price-daily"

    landing_object = queries.load_landing_object_by_id(lake, landing_id=LANDING_ID)
    assert landing_object is not None
    assert landing_object["source_uri"] == "s3://bucket/object.json"
    assert landing_object["run_status"] == "completed"
    assert landing_object["unit_status"] == "completed"

    unit_landing_objects = queries.load_landing_objects(lake, run_id=RUN_ID_COMPLETED, unit_id=UNIT_ID)
    assert len(unit_landing_objects) == 1
    assert unit_landing_objects[0]["source_uri"] == "s3://bucket/object.json"

    unrelated_landing_objects = queries.load_landing_objects(lake, run_id=RUN_ID_COMPLETED, unit_id=RUN_ID_FAILED)
    assert unrelated_landing_objects == []

    rejections = queries.load_rejections(lake, run_id=RUN_ID_COMPLETED)
    assert len(rejections) == 1
    assert rejections[0]["reason"] == "parse_error"

    unit_rejections = queries.load_rejections(lake, run_id=RUN_ID_COMPLETED, unit_id=UNIT_ID)
    assert len(unit_rejections) == 1
    assert unit_rejections[0]["reason"] == "parse_error"

    unrelated_rejections = queries.load_rejections(lake, run_id=RUN_ID_COMPLETED, unit_id=RUN_ID_FAILED)
    assert unrelated_rejections == []

    dbt_nodes = queries.load_dbt_node_results(lake, run_id=RUN_ID_COMPLETED)
    assert len(dbt_nodes) == 1
    assert dbt_nodes[0]["unique_id"] == "model.unique_stocks.fct_daily_price"

    evidence_summary = queries.load_audit_evidence_summary(lake, since=now - timedelta(days=2), domains=("eod_price",))
    assert evidence_summary == {
        "landing_objects": 1,
        "landing_bytes": 1024,
        "rejection_samples": 1,
        "coverage_records": 0,
        "dbt_invocations": 1,
        "dbt_attention_invocations": 0,
        "dbt_attention_nodes": 0,
    }


def _lake_with_dashboard_tables() -> DataLakeClient:
    lake = DataLakeClient(connection=duckdb.connect(":memory:"))
    lake.execute(
        """
        CREATE TABLE pipeline.runs (
            run_id UUID,
            parent_run_id UUID,
            prefect_flow_run_id UUID,
            flow_name VARCHAR,
            domain VARCHAR,
            run_kind VARCHAR,
            provider VARCHAR,
            environment VARCHAR,
            code_version VARCHAR,
            parameters_json JSON,
            target_window_start DATE,
            target_window_end DATE,
            status VARCHAR,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            units_total INTEGER,
            units_succeeded INTEGER,
            units_failed INTEGER,
            units_skipped INTEGER,
            rows_raw INTEGER,
            rows_valid INTEGER,
            rows_rejected INTEGER,
            rows_written INTEGER,
            summary_json JSON,
            error_class VARCHAR,
            error_message TEXT
        )
        """
    )
    lake.execute(
        """
        CREATE TABLE pipeline.run_units (
            unit_id UUID,
            run_id UUID,
            domain VARCHAR,
            provider VARCHAR,
            unit_type VARCHAR,
            unit_key_hash VARCHAR,
            unit_key_json JSON,
            status VARCHAR,
            reason VARCHAR,
            source_uri VARCHAR,
            rows_raw INTEGER,
            rows_valid INTEGER,
            rows_rejected INTEGER,
            rows_written INTEGER,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            error_class VARCHAR,
            error_message TEXT
        )
        """
    )
    lake.execute(
        """
        CREATE TABLE pipeline.landing_objects (
            landing_id UUID,
            run_id UUID,
            unit_id UUID,
            domain VARCHAR,
            provider VARCHAR,
            dataset VARCHAR,
            source_uri VARCHAR,
            partition_json JSON,
            rows_raw INTEGER,
            byte_count INTEGER,
            content_hash VARCHAR,
            recorded_at TIMESTAMPTZ
        )
        """
    )
    lake.execute(
        """
        CREATE TABLE pipeline.rejections (
            rejection_id UUID,
            run_id UUID,
            unit_id UUID,
            domain VARCHAR,
            entity_key_json JSON,
            source_uri VARCHAR,
            raw_hash VARCHAR,
            reason VARCHAR,
            error_class VARCHAR,
            error_message TEXT,
            raw_sample_json JSON,
            recorded_at TIMESTAMPTZ
        )
        """
    )
    lake.execute(
        """
        CREATE TABLE pipeline.dbt_invocations (
            dbt_run_id UUID,
            run_id UUID,
            dbt_invocation_id UUID,
            command VARCHAR,
            command_args_json JSON,
            project_dir VARCHAR,
            profiles_dir VARCHAR,
            target VARCHAR,
            status VARCHAR,
            return_code INTEGER,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            elapsed_seconds DOUBLE,
            artifact_path VARCHAR,
            artifact_metadata_json JSON,
            error_message TEXT
        )
        """
    )
    lake.execute(
        """
        CREATE TABLE pipeline.dbt_node_results (
            node_result_id UUID,
            dbt_run_id UUID,
            unique_id VARCHAR,
            resource_type VARCHAR,
            status VARCHAR,
            execution_time DOUBLE,
            failures INTEGER,
            message TEXT,
            adapter_response_json JSON,
            rows_affected INTEGER,
            relation_name VARCHAR,
            compiled BOOLEAN
        )
        """
    )
    return lake


def _insert_runs(lake: DataLakeClient, *, now: datetime) -> None:
    rows: list[dict[str, Any]] = [
        {
            "run_id": RUN_ID_COMPLETED,
            "parent_run_id": None,
            "prefect_flow_run_id": None,
            "flow_name": "eod-price-daily",
            "domain": "eod_price",
            "run_kind": "daily",
            "provider": "eodhd",
            "environment": "test",
            "code_version": "test-sha",
            "parameters_json": {"trade_date": "2026-06-03"},
            "target_window_start": None,
            "target_window_end": None,
            "status": "completed",
            "started_at": now - timedelta(hours=4),
            "completed_at": now - timedelta(hours=3, minutes=58),
            "units_total": 1,
            "units_succeeded": 1,
            "units_failed": 0,
            "units_skipped": 0,
            "rows_raw": 100,
            "rows_valid": 100,
            "rows_rejected": 0,
            "rows_written": 100,
            "summary_json": {"ok": True},
            "error_class": None,
            "error_message": None,
        },
        {
            "run_id": RUN_ID_FAILED,
            "parent_run_id": None,
            "prefect_flow_run_id": None,
            "flow_name": "eod-price-daily",
            "domain": "eod_price",
            "run_kind": "daily",
            "provider": "eodhd",
            "environment": "test",
            "code_version": "test-sha",
            "parameters_json": {"trade_date": "2026-06-03"},
            "target_window_start": None,
            "target_window_end": None,
            "status": "failed",
            "started_at": now - timedelta(hours=1),
            "completed_at": now - timedelta(minutes=59),
            "units_total": 1,
            "units_succeeded": 0,
            "units_failed": 1,
            "units_skipped": 0,
            "rows_raw": 0,
            "rows_valid": 0,
            "rows_rejected": 0,
            "rows_written": 0,
            "summary_json": {"ok": False},
            "error_class": "ValueError",
            "error_message": "bad provider response",
        },
        {
            "run_id": RUN_ID_RUNNING,
            "parent_run_id": None,
            "prefect_flow_run_id": None,
            "flow_name": "eod-price-daily",
            "domain": "eod_price",
            "run_kind": "daily",
            "provider": "eodhd",
            "environment": "test",
            "code_version": "test-sha",
            "parameters_json": {"trade_date": "2026-06-03"},
            "target_window_start": None,
            "target_window_end": None,
            "status": "running",
            "started_at": now - timedelta(hours=5),
            "completed_at": None,
            "units_total": 0,
            "units_succeeded": 0,
            "units_failed": 0,
            "units_skipped": 0,
            "rows_raw": 0,
            "rows_valid": 0,
            "rows_rejected": 0,
            "rows_written": 0,
            "summary_json": None,
            "error_class": None,
            "error_message": None,
        },
        {
            "run_id": "018f0000-0000-7000-8000-000000000099",
            "parent_run_id": None,
            "prefect_flow_run_id": None,
            "flow_name": "instrument-refresh",
            "domain": "instrument",
            "run_kind": "snapshot",
            "provider": "eodhd",
            "environment": "test",
            "code_version": "test-sha",
            "parameters_json": {"snapshot_date": "2026-05-04"},
            "target_window_start": None,
            "target_window_end": None,
            "status": "completed",
            "started_at": now - timedelta(days=30),
            "completed_at": now - timedelta(days=30, minutes=-5),
            "units_total": 1,
            "units_succeeded": 1,
            "units_failed": 0,
            "units_skipped": 0,
            "rows_raw": 10,
            "rows_valid": 10,
            "rows_rejected": 0,
            "rows_written": 10,
            "summary_json": {"ok": True},
            "error_class": None,
            "error_message": None,
        },
    ]
    lake.insert_rows("pipeline", "runs", rows)


def _insert_detail_rows(lake: DataLakeClient, *, now: datetime) -> None:
    lake.insert_rows(
        "pipeline",
        "run_units",
        [
            {
                "unit_id": UNIT_ID,
                "run_id": RUN_ID_COMPLETED,
                "domain": "eod_price",
                "provider": "eodhd",
                "unit_type": "exchange_date",
                "unit_key_hash": "hash-1",
                "unit_key_json": {"provider_exchange_code": "US"},
                "status": "completed",
                "rows_raw": 100,
                "rows_valid": 100,
                "rows_written": 100,
                "started_at": now - timedelta(hours=4),
                "completed_at": now - timedelta(hours=3, minutes=58),
            }
        ],
    )
    lake.insert_rows(
        "pipeline",
        "landing_objects",
        [
            {
                "landing_id": LANDING_ID,
                "run_id": RUN_ID_COMPLETED,
                "unit_id": UNIT_ID,
                "domain": "eod_price",
                "provider": "eodhd",
                "dataset": "eod_price.daily",
                "source_uri": "s3://bucket/object.json",
                "partition_json": {"provider_exchange_code": "US"},
                "rows_raw": 100,
                "byte_count": 1024,
                "content_hash": "abc",
                "recorded_at": now,
            }
        ],
    )
    lake.insert_rows(
        "pipeline",
        "rejections",
        [
            {
                "rejection_id": "018f0000-0000-7000-8000-000000000007",
                "run_id": RUN_ID_COMPLETED,
                "unit_id": UNIT_ID,
                "domain": "eod_price",
                "entity_key_json": {"ticker": "BAD.US"},
                "source_uri": "s3://bucket/object.json",
                "raw_hash": "raw-hash",
                "reason": "parse_error",
                "error_class": "ValueError",
                "error_message": "bad close",
                "raw_sample_json": {"close": None},
                "recorded_at": now,
            }
        ],
    )
    lake.insert_rows(
        "pipeline",
        "dbt_invocations",
        [
            {
                "dbt_run_id": DBT_RUN_ID,
                "run_id": RUN_ID_COMPLETED,
                "command": "build",
                "command_args_json": ["dbt", "build"],
                "project_dir": "dbt",
                "profiles_dir": "dbt",
                "target": "dev",
                "status": "completed",
                "return_code": 0,
                "started_at": now - timedelta(minutes=10),
                "completed_at": now - timedelta(minutes=9),
                "elapsed_seconds": 60.0,
            }
        ],
    )
    lake.insert_rows(
        "pipeline",
        "dbt_node_results",
        [
            {
                "node_result_id": "018f0000-0000-7000-8000-000000000008",
                "dbt_run_id": DBT_RUN_ID,
                "unique_id": "model.unique_stocks.fct_daily_price",
                "resource_type": "model",
                "status": "success",
                "execution_time": 1.5,
                "failures": 0,
                "rows_affected": 100,
                "relation_name": "fct_daily_price",
                "compiled": True,
            }
        ],
    )
