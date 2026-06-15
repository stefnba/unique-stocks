"""Tests for app Prefect limit definitions."""

import importlib

import pytest

from config.settings import get_settings
from control_plane.prefect import limits as app_limits
from core.orchestration import limits as orchestration_limits
from core.orchestration.limits import LAKE_WRITER_LIMIT


class FakeClient:
    """Fake Prefect client where every limit is missing."""

    async def read_global_concurrency_limit_by_name(self, name: str) -> object:
        """Pretend no Prefect global limit exists."""
        raise orchestration_limits.ObjectNotFound(Exception(f"{name} missing"))


class FakeClientContext:
    """Async context manager for fake Prefect clients."""

    async def __aenter__(self) -> FakeClient:
        """Return the fake client."""
        return FakeClient()

    async def __aexit__(self, *_: object) -> None:
        """Exit without cleanup."""


def test_app_limits_resolve_settings_and_provider_policies(monkeypatch: pytest.MonkeyPatch) -> None:
    """App limit registry should resolve settings and provider policy definitions."""
    monkeypatch.setenv("PREFECT_LAKE_WRITER_LIMIT", "3")
    get_settings.cache_clear()

    try:
        reloaded_limits = importlib.reload(app_limits)
        limits = {limit.name: limit for limit in reloaded_limits.PREFECT_LIMITS._resolve_limits()}
    finally:
        get_settings.cache_clear()
        importlib.reload(app_limits)

    assert limits[LAKE_WRITER_LIMIT].limit == 3
    assert limits["unique-stocks.provider.eodhd"].limit == 100
    assert limits["unique-stocks.provider.eodhd"].slot_decay_per_second == 10.0
    assert "unique-stocks.provider.iso10383" not in limits


@pytest.mark.asyncio
async def test_app_limits_dry_run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Dry-run should describe managed app Prefect limits."""
    monkeypatch.setattr(orchestration_limits, "get_client", lambda: FakeClientContext())

    exit_code = await app_limits.PREFECT_LIMITS.sync(dry_run=True)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Would create global limit unique-stocks.lake-writer" in output
    assert "Would create global limit unique-stocks.provider.eodhd" in output
