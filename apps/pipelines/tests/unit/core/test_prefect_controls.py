"""Tests for lightweight Prefect control helpers."""

from collections.abc import Generator
from contextlib import contextmanager

import pytest

from core import prefect_controls


def test_lake_writer_limit_fails_open_when_not_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local bootstrap should continue if Prefect global limits are not ready."""

    @contextmanager
    def unavailable_limit(*_: object, **__: object) -> Generator[None]:
        raise RuntimeError("limit missing")
        yield

    monkeypatch.delenv("PREFECT_GLOBAL_LIMITS_STRICT", raising=False)
    monkeypatch.setattr(prefect_controls, "concurrency", unavailable_limit)

    with prefect_controls.lake_writer_limit("test"):
        observed = True

    assert observed is True


def test_lake_writer_limit_raises_when_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production can fail closed when global limits are expected to exist."""

    @contextmanager
    def unavailable_limit(*_: object, **__: object) -> Generator[None]:
        raise RuntimeError("limit missing")
        yield

    monkeypatch.setenv("PREFECT_GLOBAL_LIMITS_STRICT", "true")
    monkeypatch.setattr(prefect_controls, "concurrency", unavailable_limit)

    with pytest.raises(RuntimeError, match="limit missing"), prefect_controls.lake_writer_limit("test"):
        pass


@pytest.mark.asyncio
async def test_wait_for_provider_api_credit_fails_open_when_not_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provider calls should continue locally if the Prefect limit is absent."""
    calls: list[dict[str, object]] = []

    async def unavailable_rate_limit(*args: object, **kwargs: object) -> None:
        calls.append({"args": args, "kwargs": kwargs})
        raise RuntimeError("limit missing")

    monkeypatch.delenv("PREFECT_GLOBAL_LIMITS_STRICT", raising=False)
    monkeypatch.setattr(prefect_controls, "rate_limit", unavailable_rate_limit)

    await prefect_controls.wait_for_provider_api_credit(provider="demo", operation="GET /prices")

    assert calls


def test_selected_dbt_asset_groups_defaults_to_all_for_full_build() -> None:
    """A full dbt build should observe all configured Silver/Gold asset groups."""
    assert prefect_controls._selected_dbt_asset_groups([]) == [
        "exchange",
        "instrument",
        "price",
        "fundamental",
    ]
