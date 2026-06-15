"""Tests for the Prefect limits setup CLI adapter."""

import pytest

from scripts.prefect import setup_limits


def test_setup_limits_main_uses_app_limit_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """The limits CLI should invoke the explicit app-owned limit registry."""
    calls: list[bool] = []

    class FakePrefectLimits:
        async def sync(self, *, dry_run: bool) -> int:
            calls.append(dry_run)
            return 0

    monkeypatch.setattr(setup_limits, "PREFECT_LIMITS", FakePrefectLimits())

    exit_code = setup_limits.main(["--dry-run"])

    assert exit_code == 0
    assert calls == [True]
