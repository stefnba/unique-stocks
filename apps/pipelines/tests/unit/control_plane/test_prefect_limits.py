"""Tests for app Prefect limit definitions."""

import pytest

from config.settings import get_settings
from control_plane.prefect import limits as app_limits
from core.global_limits import LAKE_WRITER_LIMIT, provider_rate_limit_name
from core.orchestration import limits as orchestration_limits


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
    assert limits[provider_rate_limit_name("eodhd")].limit == 100
    assert limits[provider_rate_limit_name("eodhd")].slot_decay_per_second == 25.0
    assert provider_rate_limit_name("iso10383") not in limits


@pytest.mark.asyncio
async def test_app_limits_plan(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Plan mode should describe managed app Prefect limits."""
    monkeypatch.setattr(orchestration_limits, "get_client", lambda: FakeClientContext())

    exit_code = await app_limits.PREFECT_LIMITS.sync(plan=True)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert f"Would create global limit {LAKE_WRITER_LIMIT}" in output
    assert f"Would create global limit {provider_rate_limit_name('eodhd')}" in output
