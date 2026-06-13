"""Tests for Prefect limit setup."""

import pytest

from core.prefect.limits import setup_prefect_limits
from providers.registry import Provider


@pytest.mark.asyncio
async def test_setup_prefect_limits_uses_app_defaults(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Dry-run should use app-declared provider rate-limit policies."""
    monkeypatch.delenv("PREFECT_LAKE_WRITER_LIMIT", raising=False)

    exit_code = await setup_prefect_limits(providers=Provider, dry_run=True)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "unique-stocks.lake-writer: limit=1" in output
    assert "unique-stocks.provider.eodhd: burst_capacity=100" in output
    assert "slot_decay_per_second=10.0" in output


@pytest.mark.asyncio
async def test_setup_prefect_limits_honors_lake_writer_env_override(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Lake writer limit remains environment-overridable for deployments."""
    monkeypatch.setenv("PREFECT_LAKE_WRITER_LIMIT", "2")

    exit_code = await setup_prefect_limits(providers=Provider, dry_run=True)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "unique-stocks.lake-writer: limit=2" in output
    assert "unique-stocks.provider.eodhd: burst_capacity=100" in output
