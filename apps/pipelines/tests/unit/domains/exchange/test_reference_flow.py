"""Tests for the exchange reference parent flow."""

from datetime import date

import pytest
from pytest import MonkeyPatch

from orchestration.flows import exchange as exchange_flows


@pytest.mark.asyncio
async def test_exchange_reference_refresh_runs_schedule_after_prebuild(monkeypatch: MonkeyPatch) -> None:
    """The parent reference flow should run schedules only after the exchange contract build succeeds."""
    calls: list[str] = []

    async def fake_catalog() -> int:
        calls.append("catalog")
        return 72

    async def fake_mic_registry(*, snapshot_date: date | None = None) -> dict[str, object]:
        calls.append(f"mic:{snapshot_date}")
        return {"rows_written": 2856}

    async def fake_build(**kwargs: object) -> dict[str, object]:
        calls.append(f"build:{kwargs['build']}")
        return {"triggered": True}

    async def fake_schedule(**kwargs: object) -> dict[str, object]:
        calls.append("schedule")
        return {"scope": {"selected_provider_schedule_codes": 3}, "params": kwargs}

    monkeypatch.setattr(exchange_flows, "exchange_catalog_flow", fake_catalog)
    monkeypatch.setattr(exchange_flows, "exchange_mic_registry_flow", fake_mic_registry)
    monkeypatch.setattr(exchange_flows, "run_dbt_build_deployment", fake_build)
    monkeypatch.setattr(exchange_flows, "exchange_schedule_flow", fake_schedule)

    summary = await exchange_flows.exchange_reference_refresh_flow.fn(
        snapshot_date=date(2026, 6, 12),
        schedule_batch_size=4,
        provider_batch_delay_seconds=1.5,
        run_dbt_build=True,
    )

    assert calls == ["catalog", "mic:2026-06-12", "build:exchange-build", "schedule"]
    assert summary["exchange_catalog_rows_written"] == 72
    assert summary["exchange_mic_registry"] == {"rows_written": 2856}
    assert summary["exchange_build_before_schedule"] == {"triggered": True}
    assert summary["exchange_schedule"] == {
        "scope": {"selected_provider_schedule_codes": 3},
        "params": {
            "snapshot_date": date(2026, 6, 12),
            "batch_size": 4,
            "provider_batch_delay_seconds": 1.5,
            "run_dbt_build": True,
        },
    }


@pytest.mark.asyncio
async def test_exchange_reference_refresh_stops_before_schedule_when_prebuild_fails(
    monkeypatch: MonkeyPatch,
) -> None:
    """A failed pre-schedule exchange-build should explain empty schedule Bronze tables."""
    calls: list[str] = []

    async def fake_catalog() -> int:
        calls.append("catalog")
        return 72

    async def fake_mic_registry(*, snapshot_date: date | None = None) -> dict[str, object]:
        calls.append(f"mic:{snapshot_date}")
        return {"rows_written": 2856}

    async def fake_build(**_: object) -> dict[str, object]:
        calls.append("build")
        raise RuntimeError("pre-schedule dbt failed")

    async def fake_schedule(**_: object) -> dict[str, object]:
        calls.append("schedule")
        return {}

    monkeypatch.setattr(exchange_flows, "exchange_catalog_flow", fake_catalog)
    monkeypatch.setattr(exchange_flows, "exchange_mic_registry_flow", fake_mic_registry)
    monkeypatch.setattr(exchange_flows, "run_dbt_build_deployment", fake_build)
    monkeypatch.setattr(exchange_flows, "exchange_schedule_flow", fake_schedule)

    with pytest.raises(RuntimeError, match="pre-schedule dbt failed"):
        await exchange_flows.exchange_reference_refresh_flow.fn(snapshot_date=date(2026, 6, 12))

    assert calls == ["catalog", "mic:2026-06-12", "build"]
