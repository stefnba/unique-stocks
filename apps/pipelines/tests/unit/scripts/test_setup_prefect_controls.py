"""Tests for Prefect controls setup helper."""

import pytest

from scripts import setup_prefect_controls


@pytest.mark.asyncio
async def test_setup_prefect_controls_dry_run(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Dry-run should describe limits and automations without contacting Prefect."""
    monkeypatch.setenv("PREFECT_LAKE_WRITER_LIMIT", "2")
    monkeypatch.setenv("PREFECT_PROVIDER_API_CREDIT_LIMIT", "5")
    monkeypatch.setenv("PREFECT_PROVIDER_API_CREDIT_DECAY_PER_SECOND", "0.5")

    exit_code = await setup_prefect_controls.setup_prefect_controls(dry_run=True)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "unique-stocks.lake-writer: limit=2" in output
    assert "unique-stocks.provider-api-credit: limit=5" in output
    assert "slot_decay_per_second=0.5" in output
    assert "unique-stocks dbt failure alert" in output


def test_automation_action_uses_notification_block_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Operators can turn event automations into notifications with one block id."""
    block_id = "018f0000-0000-7000-8000-000000000001"
    monkeypatch.setenv("PREFECT_NOTIFICATION_BLOCK_ID", block_id)

    action = setup_prefect_controls._automation_action(name="dbt alert")

    assert action.type == "send-notification"
    assert str(action.block_document_id) == block_id
