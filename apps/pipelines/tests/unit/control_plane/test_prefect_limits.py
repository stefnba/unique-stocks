"""Tests for app Prefect limit definitions."""

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


def test_app_lake_writer_slots_are_environment_policy() -> None:
    """Lake writer slots should be explicit control-plane policy."""
    assert app_limits.LAKE_WRITER_SLOTS_BY_ENVIRONMENT == {
        "dev": 1,
        "docker_dev": 1,
        "prod": 1,
    }


def test_app_limits_define_lake_writer_and_provider_policies() -> None:
    """App limit registry should define lake writer and provider policy definitions."""
    limits = {limit.name: limit for limit in app_limits.PREFECT_LIMITS._resolve_limits()}
    lake_writer_slots = app_limits.LAKE_WRITER_SLOTS_BY_ENVIRONMENT[get_settings().environment]

    assert limits[LAKE_WRITER_LIMIT].limit == lake_writer_slots
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
