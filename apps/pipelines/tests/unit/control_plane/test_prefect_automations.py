"""Tests for app Prefect automation definitions."""

import pytest
from prefect.automations import Automation
from prefect.events.actions import DoNothing

from control_plane.prefect.automations import PREFECT_AUTOMATIONS
from core.prefect.events import PrefectEvent

EXPECTED_AUTOMATIONS = {
    "unique-stocks dbt failure alert": PrefectEvent.DBT_FAILED,
    "unique-stocks coverage gate alert": PrefectEvent.COVERAGE_GATE_FAILED,
    "unique-stocks ingestion partial alert": PrefectEvent.INGESTION_PARTIAL,
    "unique-stocks ingestion failure alert": PrefectEvent.INGESTION_FAILED,
    "unique-stocks stale running audit alert": PrefectEvent.PIPELINE_STALE_RUNNING,
    "unique-stocks cancellation audit alert": PrefectEvent.PIPELINE_CANCELLED,
}


@pytest.mark.asyncio
async def test_app_automations_plan(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Plan mode should describe managed app event automations."""

    async def read_missing(*, name: str) -> Automation:
        raise ValueError(f"Automation with name {name!r} not found")

    monkeypatch.setattr(Automation, "aread", read_missing)

    exit_code = await PREFECT_AUTOMATIONS.sync(plan=True)

    output = capsys.readouterr().out
    assert exit_code == 0
    for automation_name in EXPECTED_AUTOMATIONS:
        assert f"Would create automation: {automation_name}" in output


def test_app_automations_define_expected_triggers() -> None:
    """App automation definitions should target canonical Prefect events."""
    automations_by_name = {automation.name: automation for automation in PREFECT_AUTOMATIONS.automations}

    assert set(automations_by_name) == set(EXPECTED_AUTOMATIONS)
    for automation_name, event in EXPECTED_AUTOMATIONS.items():
        automation = automations_by_name[automation_name]
        payload = automation.model_dump(mode="json", exclude_unset=True)

        assert payload["trigger"]["expect"] == [event.value]
        assert payload["tags"] == ["unique-stocks", "pipelines"]
        assert isinstance(automation.actions[0], DoNothing)
