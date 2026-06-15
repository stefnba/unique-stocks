"""Tests for the Prefect deployment setup CLI adapter."""

from pathlib import Path

import pytest

from scripts.orchestration import setup_deployments


def test_setup_deployments_main_uses_plan_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """The deployment setup CLI should pass plan mode to the sync implementation."""
    calls: list[dict[str, object]] = []

    async def sync_deployments_stub(
        *,
        prefect_yaml: Path,
        plan: bool,
        prune_only: bool,
    ) -> int:
        calls.append(
            {
                "prefect_yaml": prefect_yaml,
                "plan": plan,
                "prune_only": prune_only,
            }
        )
        return 0

    monkeypatch.setattr(setup_deployments, "sync_deployments", sync_deployments_stub)

    exit_code = setup_deployments.main(["--plan", "--prune-only"])

    assert exit_code == 0
    assert calls == [
        {
            "prefect_yaml": setup_deployments.DEFAULT_PREFECT_YAML,
            "plan": True,
            "prune_only": True,
        }
    ]
