"""Tests for lake writer Prefect runtime guards."""

from collections.abc import Generator
from contextlib import contextmanager

import pytest

from core.global_limits import PREFECT_GLOBAL_LIMITS_FAIL_CLOSED_ENV_VAR, prefect_global_limits_fail_closed
from core.lake import limits as lake_limits


def test_lake_writer_limit_fails_open_when_fail_closed_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local bootstrap should continue if Prefect global limits are not ready."""

    @contextmanager
    def unavailable_limit(*_: object, **__: object) -> Generator[None]:
        raise RuntimeError("limit missing")
        yield

    monkeypatch.delenv(PREFECT_GLOBAL_LIMITS_FAIL_CLOSED_ENV_VAR, raising=False)
    monkeypatch.setattr(lake_limits, "_lake_writer_limit_missing", False)
    monkeypatch.setattr(lake_limits, "concurrency", unavailable_limit)

    with lake_limits.lake_writer_limit("test"):
        observed = True

    assert observed is True

    with lake_limits.lake_writer_limit("test"):
        observed_again = True

    assert observed_again is True


def test_lake_writer_limit_raises_when_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production can fail closed when global limits are expected to exist."""

    @contextmanager
    def unavailable_limit(*_: object, **__: object) -> Generator[None]:
        raise RuntimeError("limit missing")
        yield

    monkeypatch.setenv(PREFECT_GLOBAL_LIMITS_FAIL_CLOSED_ENV_VAR, "true")
    monkeypatch.setattr(lake_limits, "_lake_writer_limit_missing", False)
    monkeypatch.setattr(lake_limits, "concurrency", unavailable_limit)

    with (
        pytest.raises(RuntimeError, match="limit missing"),
        lake_limits.lake_writer_limit("test"),
    ):
        pass


def test_prefect_global_limits_fail_closed_reads_explicit_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operators can fail closed after global limits are bootstrapped."""
    monkeypatch.setenv(PREFECT_GLOBAL_LIMITS_FAIL_CLOSED_ENV_VAR, "true")

    assert prefect_global_limits_fail_closed() is True
