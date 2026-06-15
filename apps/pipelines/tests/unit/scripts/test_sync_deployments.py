"""Tests for the Prefect deployment sync CLI adapter."""

from pathlib import Path

import pytest

from scripts.prefect import sync_deployments


def test_sync_deployments_main_uses_plan_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """The deployment sync CLI should pass plan mode to the sync implementation."""
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

    monkeypatch.setattr(sync_deployments, "sync_deployments", sync_deployments_stub)

    exit_code = sync_deployments.main(["--plan", "--prune-only"])

    assert exit_code == 0
    assert calls == [
        {
            "prefect_yaml": sync_deployments.DEFAULT_PREFECT_YAML,
            "plan": True,
            "prune_only": True,
        }
    ]
