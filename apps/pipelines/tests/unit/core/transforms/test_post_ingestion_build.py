"""Tests for app post-ingestion dbt build gating."""

from dataclasses import dataclass
from datetime import date
from uuid import UUID

import pytest
from pydantic import SecretStr

from config.settings import Settings
from domains.instrument.contracts import InstrumentRefreshRequest, InstrumentRefreshResult
from orchestration import post_ingestion
from orchestration.flows import instrument as instrument_flows


@dataclass(frozen=True)
class FakeState:
    """Minimal Prefect state double."""

    name: str
    type: str
    completed: bool

    def is_completed(self) -> bool:
        """Return whether the fake state is completed."""
        return self.completed


@dataclass(frozen=True)
class FakeFlowRun:
    """Minimal Prefect flow run double."""

    id: UUID
    state: FakeState | None


@pytest.mark.asyncio
async def test_post_ingestion_build_runs_deployment_after_completed_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """A completed upstream audit status should launch the matching dbt deployment."""
    calls: list[dict[str, object]] = []
    events: list[str] = []

    monkeypatch.setattr(post_ingestion, "get_settings", lambda: Settings(motherduck_token=SecretStr("")))

    def fake_reset_lake_client() -> None:
        events.append("release")

    monkeypatch.setattr(post_ingestion, "reset_lake_client", fake_reset_lake_client)

    async def fake_run_deployment(*args: object, **kwargs: object) -> FakeFlowRun:
        events.append("submit")
        calls.append({"args": args, "kwargs": kwargs})
        return FakeFlowRun(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            state=FakeState(name="Completed", type="COMPLETED", completed=True),
        )

    monkeypatch.setattr(post_ingestion, "arun_deployment", fake_run_deployment)

    result = await post_ingestion.run_dbt_build_after_ingestion(
        enabled=True,
        build="price-build",
        upstream_status="completed",
        parent_run_id="parent-run",
    )

    assert result["triggered"] is True
    assert result["deployment"] == "dbt-build/price-build"
    assert result["flow_run_id"] == "00000000-0000-0000-0000-000000000001"
    assert calls == [
        {
            "args": ("dbt-build/price-build",),
            "kwargs": {
                "parameters": {"parent_run_id": "parent-run"},
                "idempotency_key": "parent-run:price-build",
                "tags": ["post-ingestion-dbt", "price-build"],
                "as_subflow": True,
            },
        }
    ]
    assert events == ["release", "submit"]


@pytest.mark.asyncio
async def test_post_ingestion_build_skips_partial_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Partial upstream audit status should not promote Bronze data."""

    async def fail_if_called(*_: object, **__: object) -> FakeFlowRun:
        raise AssertionError("partial ingestion must not trigger dbt")

    monkeypatch.setattr(post_ingestion, "arun_deployment", fail_if_called)

    result = await post_ingestion.run_dbt_build_after_ingestion(
        enabled=True,
        build="fundamental-build",
        upstream_status="partial",
        parent_run_id="parent-run",
    )

    assert result == {
        "enabled": True,
        "triggered": False,
        "build": "fundamental-build",
        "reason": "upstream_not_completed",
        "upstream_status": "partial",
    }


@pytest.mark.asyncio
async def test_post_ingestion_build_raises_when_dbt_deployment_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """The launcher should fail the caller when the dbt deployment does not complete."""
    monkeypatch.setattr(post_ingestion, "get_settings", lambda: Settings(motherduck_token=SecretStr("")))
    monkeypatch.setattr(post_ingestion, "reset_lake_client", lambda: None)

    async def fake_run_deployment(*_: object, **__: object) -> FakeFlowRun:
        return FakeFlowRun(
            id=UUID("00000000-0000-0000-0000-000000000002"),
            state=FakeState(name="Failed", type="FAILED", completed=False),
        )

    monkeypatch.setattr(post_ingestion, "arun_deployment", fake_run_deployment)

    with pytest.raises(RuntimeError, match="dbt-build/instrument-build"):
        await post_ingestion.run_dbt_build_after_ingestion(
            enabled=True,
            build="instrument-build",
            upstream_status="completed",
            parent_run_id="parent-run",
        )


@pytest.mark.asyncio
async def test_instrument_flow_triggers_build_with_completed_audit_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Instrument flow should pass durable audit status into the post-ingestion build gate."""
    build_calls: list[dict[str, object]] = []
    service_requests: list[InstrumentRefreshRequest] = []

    async def fake_refresh(request: InstrumentRefreshRequest) -> InstrumentRefreshResult:
        service_requests.append(request)
        return InstrumentRefreshResult(
            run_id="instrument-run",
            status="completed",
            summary={"snapshot_date": "2026-06-01", "exchange": {"US": {"rows": 1}}},
        )

    async def fake_build(**kwargs: object) -> dict[str, object]:
        build_calls.append(kwargs)
        return {"triggered": True}

    monkeypatch.setattr(instrument_flows, "run_instrument_refresh", fake_refresh)
    monkeypatch.setattr(instrument_flows, "run_dbt_build_after_ingestion", fake_build)

    summary = await instrument_flows.instrument_flow.fn(
        snapshot_date=date(2026, 6, 1),
        provider_exchange_codes=["US"],
        run_dbt_build=True,
    )

    assert summary["dbt_build"] == {"triggered": True}
    assert service_requests == [
        InstrumentRefreshRequest(
            snapshot_date=date(2026, 6, 1),
            provider_exchange_codes=["US"],
            run_dbt_build=True,
        )
    ]
    assert build_calls == [
        {
            "enabled": True,
            "build": "instrument-build",
            "upstream_status": "completed",
            "parent_run_id": "instrument-run",
        }
    ]
