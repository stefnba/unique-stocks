"""Tests for Prefect automation setup helpers."""

import pytest

from core.prefect.automations import build_prefect_automation, setup_prefect_automations


@pytest.mark.asyncio
async def test_setup_prefect_automations_dry_run(capsys: pytest.CaptureFixture[str]) -> None:
    """Dry-run should describe managed event automations."""
    exit_code = await setup_prefect_automations(dry_run=True)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "unique-stocks dbt failure alert" in output
    assert "on trigger: do-nothing" in output


def test_automation_puts_action_on_trigger(monkeypatch: pytest.MonkeyPatch) -> None:
    """Automation payloads should use the trigger-action field rendered by Prefect UI."""
    monkeypatch.delenv("PREFECT_NOTIFICATION_BLOCK_ID", raising=False)

    automation = build_prefect_automation(
        name="dbt alert",
        event="unique-stocks.dbt.failed",
        description="dbt failed",
    )
    payload = automation.model_dump(mode="json", exclude_unset=True)

    assert payload["actions"] == []
    assert payload["actions_on_trigger"] == [{"type": "do-nothing"}]


def test_automation_trigger_action_uses_notification_block_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configured notification blocks should become visible trigger actions."""
    block_id = "018f0000-0000-7000-8000-000000000001"
    monkeypatch.setenv("PREFECT_NOTIFICATION_BLOCK_ID", block_id)

    automation = build_prefect_automation(
        name="dbt alert",
        event="unique-stocks.dbt.failed",
        description="dbt failed",
    )
    payload = automation.model_dump(mode="json", exclude_unset=True)

    assert payload["actions"] == []
    assert payload["actions_on_trigger"][0]["type"] == "send-notification"
    assert payload["actions_on_trigger"][0]["block_document_id"] == block_id
