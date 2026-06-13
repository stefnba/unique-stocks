"""Tests for combined Prefect controls setup helper."""

import pytest

from scripts import setup_prefect_controls


@pytest.mark.asyncio
async def test_setup_prefect_controls_runs_limits_and_automations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Combined setup should run limits first, then automations."""
    calls: list[str] = []

    async def setup_limits(**kwargs: object) -> int:
        calls.append(f"limits:{kwargs['dry_run']}")
        return 0

    async def setup_automations(**kwargs: object) -> int:
        calls.append(f"automations:{kwargs['dry_run']}")
        return 0

    monkeypatch.setattr(setup_prefect_controls, "setup_prefect_limits", setup_limits)
    monkeypatch.setattr(setup_prefect_controls, "setup_prefect_automations", setup_automations)

    exit_code = await setup_prefect_controls.setup_prefect_controls(dry_run=True)

    assert exit_code == 0
    assert calls == ["limits:True", "automations:True"]


@pytest.mark.asyncio
async def test_setup_prefect_controls_returns_nonzero_when_substep_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Combined setup should return the highest substep exit code."""

    async def setup_limits(**_: object) -> int:
        return 0

    async def setup_automations(**_: object) -> int:
        return 2

    monkeypatch.setattr(setup_prefect_controls, "setup_prefect_limits", setup_limits)
    monkeypatch.setattr(setup_prefect_controls, "setup_prefect_automations", setup_automations)

    assert await setup_prefect_controls.setup_prefect_controls(dry_run=False) == 2
