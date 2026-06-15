"""Tests for app Prefect automation definitions."""

from uuid import uuid4

import pytest
from prefect.automations import Automation
from prefect.events.actions import DoNothing, SendNotification

from control_plane.prefect import automations as app_automations
from core.orchestration.events import PrefectEvent

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
    monkeypatch.setattr(app_automations.BlockRegistry.SLACK_WEBHOOK, "enabled", False)

    async def fake_read(cls: type[Automation], id: object | None = None, name: str | None = None) -> Automation:
        raise ValueError(f"Automation with name {name!r} not found")

    monkeypatch.setattr(Automation, "aread", classmethod(fake_read))

    exit_code = await app_automations.PREFECT_AUTOMATIONS.sync(plan=True)

    output = capsys.readouterr().out
    assert exit_code == 0
    for automation_name in EXPECTED_AUTOMATIONS:
        assert f"Would create automation: {automation_name}" in output


def test_app_automations_define_expected_triggers(monkeypatch: pytest.MonkeyPatch) -> None:
    """App automation definitions should target canonical Prefect events."""
    monkeypatch.setattr(app_automations.BlockRegistry.SLACK_WEBHOOK, "enabled", False)

    automations_by_name = {
        automation.name: automation for automation in app_automations.PREFECT_AUTOMATIONS.automations
    }

    assert set(automations_by_name) == set(EXPECTED_AUTOMATIONS)
    for automation_name, event in EXPECTED_AUTOMATIONS.items():
        automation = automations_by_name[automation_name]
        payload = automation.model_dump(mode="json", exclude_unset=True)

        assert payload["trigger"]["expect"] == [event.value]
        assert payload["tags"] == ["unique-stocks", "pipelines"]
        assert isinstance(automation.actions[0], DoNothing)


def test_app_automation_declaration_defers_slack_document_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Declaring app automations should not require saved Prefect blocks."""

    def fail_document_id() -> object:
        raise AssertionError("Slack block document id should not be resolved during declaration")

    monkeypatch.setattr(app_automations.BlockRegistry.SLACK_WEBHOOK, "enabled", True)
    monkeypatch.setattr(app_automations.BlockRegistry.SLACK_WEBHOOK, "document_id", fail_document_id)

    registry = app_automations.define_automations(
        [
            app_automations.PipelineAlertAutomation(
                name="demo alert",
                description="demo",
                event=PrefectEvent.DBT_FAILED,
            )
        ]
    )

    assert registry.definitions[0].name == "demo alert"


def test_pipeline_alert_automation_resolves_slack_document_id_when_materialized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Slack notifications should resolve the saved block only when materialized."""
    block_document_id = uuid4()
    monkeypatch.setattr(app_automations.BlockRegistry.SLACK_WEBHOOK, "enabled", True)
    monkeypatch.setattr(app_automations.BlockRegistry.SLACK_WEBHOOK, "document_id", lambda: block_document_id)

    automation = app_automations.PipelineAlertAutomation(
        name="demo alert",
        description="demo",
        event=PrefectEvent.DBT_FAILED,
    ).to_prefect_automation()

    assert isinstance(automation.actions[0], SendNotification)
    assert automation.actions[0].block_document_id == block_document_id
