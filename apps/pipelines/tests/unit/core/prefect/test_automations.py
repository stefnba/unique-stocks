"""Tests for Prefect automation setup helpers."""

import pytest

from core.prefect.automations import (
    PrefectAutomationDefinition,
    build_prefect_automation,
    define_automations,
    notification_or_noop_action,
    setup_prefect_automations,
)


@pytest.mark.asyncio
async def test_setup_prefect_automations_dry_run(capsys: pytest.CaptureFixture[str]) -> None:
    """Dry-run should describe supplied event automations."""
    automation = build_prefect_automation(
        name="dbt alert",
        event="demo.dbt.failed",
        description="dbt failed",
    )
    exit_code = await setup_prefect_automations(automations=[automation], dry_run=True)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "dbt alert" in output
    assert "on trigger: do-nothing" in output


def test_automation_puts_action_on_trigger() -> None:
    """Automation payloads should use the trigger-action field rendered by Prefect UI."""
    automation = build_prefect_automation(
        name="dbt alert",
        event="demo.dbt.failed",
        description="dbt failed",
    )
    payload = automation.model_dump(mode="json", exclude_unset=True)

    assert payload["actions"] == []
    assert payload["actions_on_trigger"] == [{"type": "do-nothing"}]


def test_notification_action_uses_notification_block_when_configured() -> None:
    """Configured notification blocks should become visible trigger actions."""
    block_id = "018f0000-0000-7000-8000-000000000001"

    automation = build_prefect_automation(
        name="dbt alert",
        event="demo.dbt.failed",
        description="dbt failed",
        action=notification_or_noop_action(block_id=block_id, subject="dbt alert"),
    )
    payload = automation.model_dump(mode="json", exclude_unset=True)

    assert payload["actions"] == []
    assert payload["actions_on_trigger"][0]["type"] == "send-notification"
    assert payload["actions_on_trigger"][0]["block_document_id"] == block_id


@pytest.mark.asyncio
async def test_automation_registry_sets_up_definitions(capsys: pytest.CaptureFixture[str]) -> None:
    """Automation registries should build and set up declared definitions."""
    registry = define_automations(
        tags=("demo",),
        automations=[
            PrefectAutomationDefinition(
                name="dbt alert",
                event="demo.dbt.failed",
                description="dbt failed",
            )
        ],
    )

    exit_code = await registry.setup(dry_run=True)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "dbt alert" in output
    assert registry.build()[0].tags == ["demo"]


def test_automation_registry_uses_notification_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """Automation registries should resolve notification block IDs lazily."""
    block_id = "018f0000-0000-7000-8000-000000000001"
    monkeypatch.setenv("DEMO_NOTIFICATION_BLOCK_ID", block_id)
    registry = define_automations(
        notification_block_id_env_var="DEMO_NOTIFICATION_BLOCK_ID",
        automations=[
            PrefectAutomationDefinition(
                name="dbt alert",
                event="demo.dbt.failed",
                description="dbt failed",
            )
        ],
    )

    payload = registry.build()[0].model_dump(mode="json", exclude_unset=True)

    assert payload["actions_on_trigger"][0]["type"] == "send-notification"
    assert payload["actions_on_trigger"][0]["block_document_id"] == block_id
