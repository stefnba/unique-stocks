"""Tests for the Prefect automations setup CLI adapter."""

import pytest

from scripts.orchestration import setup_automations


def test_setup_automations_main_uses_plan_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """The automations CLI should plan through the explicit app-owned registry."""
    calls: list[bool] = []

    class FakePrefectAutomations:
        async def sync(self, *, plan: bool) -> int:
            calls.append(plan)
            return 0

    monkeypatch.setattr(setup_automations, "PREFECT_AUTOMATIONS", FakePrefectAutomations())

    exit_code = setup_automations.main(["--plan"])

    assert exit_code == 0
    assert calls == [True]
