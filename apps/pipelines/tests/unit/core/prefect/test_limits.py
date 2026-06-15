"""Tests for Prefect limit setup."""

from collections.abc import Generator
from contextlib import contextmanager

import pytest

from core.http.base import HttpClientBase
from core.prefect import limits as prefect_limits
from core.prefect.limits import (
    ProviderRateLimitPolicy,
    define_limits,
    provider_rate_limit_registrations,
    setup_prefect_limits,
)


class FakePrefectClient:
    """Capture global concurrency limit upserts."""

    def __init__(self) -> None:
        """Create an empty fake Prefect client."""
        self.upserts: list[dict[str, object]] = []

    async def upsert_global_concurrency_limit_by_name(
        self,
        *,
        name: str,
        limit: int,
        slot_decay_per_second: float,
    ) -> None:
        """Capture one upsert call."""
        self.upserts.append(
            {
                "name": name,
                "limit": limit,
                "slot_decay_per_second": slot_decay_per_second,
            }
        )


class ClientWithPolicy(HttpClientBase):
    """Minimal provider client with a declared rate-limit policy."""

    PROVIDER = "eodhd"
    BASE_URL = "https://example.test"
    RATE_LIMIT_POLICY = ProviderRateLimitPolicy(burst_capacity=100, slot_decay_per_second=10.0)


class ClientWithoutPolicy(HttpClientBase):
    """Minimal provider client without a rate-limit policy."""

    PROVIDER = "iso10383"
    BASE_URL = "https://example.test"


@pytest.mark.asyncio
async def test_provider_rate_limit_policy_registers_with_prefect_client() -> None:
    """Provider policy registration should upsert a Prefect rate-limit object."""
    client = FakePrefectClient()
    policy = ProviderRateLimitPolicy(burst_capacity=100, slot_decay_per_second=10.0)

    registration = await policy.register(client, provider="eodhd")

    assert registration.name == "unique-stocks.provider.eodhd"
    assert client.upserts == [
        {
            "name": "unique-stocks.provider.eodhd",
            "limit": 100,
            "slot_decay_per_second": 10.0,
        }
    ]


def test_provider_rate_limit_policy_allows_explicit_name() -> None:
    """Provider policies can use an explicit Prefect limit name when needed."""
    policy = ProviderRateLimitPolicy(
        name="unique-stocks.provider.custom",
        burst_capacity=5,
        slot_decay_per_second=0.5,
    )

    assert policy.limit_name("demo") == "unique-stocks.provider.custom"


def test_provider_rate_limit_registrations_include_only_declared_policies() -> None:
    """Only clients declaring RATE_LIMIT_POLICY should produce Prefect registrations."""
    registrations = list(provider_rate_limit_registrations((ClientWithPolicy, ClientWithoutPolicy)))

    assert [registration.name for registration in registrations] == ["unique-stocks.provider.eodhd"]
    assert registrations[0].burst_capacity == 100
    assert registrations[0].slot_decay_per_second == 10.0


def test_prefect_limit_registry_resolves_lazily() -> None:
    """Limit registries should resolve callables only when used."""
    lake_limit = 2
    registry = define_limits(
        lake_writer_limit=lambda: lake_limit,
        provider_clients=lambda: (ClientWithPolicy, ClientWithoutPolicy),
    )

    lake_limit = 4

    assert registry.resolve_lake_writer_limit() == 4
    assert registry.resolve_provider_clients() == (ClientWithPolicy, ClientWithoutPolicy)


def test_prefect_limit_registry_rejects_invalid_lake_limit() -> None:
    """Invalid lake writer limits should fail before hitting Prefect."""
    registry = define_limits(lake_writer_limit=0)

    with pytest.raises(ValueError, match="at least 1"):
        registry.resolve_lake_writer_limit()


def test_lake_writer_limit_fails_open_when_not_strict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local bootstrap should continue if Prefect global limits are not ready."""

    @contextmanager
    def unavailable_limit(*_: object, **__: object) -> Generator[None]:
        raise RuntimeError("limit missing")
        yield

    monkeypatch.delenv("PREFECT_GLOBAL_LIMITS_STRICT", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setattr(prefect_limits, "_lake_writer_limit_missing", False)
    monkeypatch.setattr(prefect_limits, "concurrency", unavailable_limit)

    with prefect_limits.lake_writer_limit("test"):
        observed = True

    assert observed is True

    with prefect_limits.lake_writer_limit("test"):
        observed_again = True

    assert observed_again is True


def test_lake_writer_limit_raises_when_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production can fail closed when global limits are expected to exist."""

    @contextmanager
    def unavailable_limit(*_: object, **__: object) -> Generator[None]:
        raise RuntimeError("limit missing")
        yield

    monkeypatch.setenv("PREFECT_GLOBAL_LIMITS_STRICT", "true")
    monkeypatch.setattr(prefect_limits, "_lake_writer_limit_missing", False)
    monkeypatch.setattr(prefect_limits, "concurrency", unavailable_limit)

    with (
        pytest.raises(RuntimeError, match="limit missing"),
        prefect_limits.lake_writer_limit("test"),
    ):
        pass


def test_strict_limits_defaults_to_false_for_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First-run bootstrap should fail open unless strict mode is explicit."""
    monkeypatch.delenv("PREFECT_GLOBAL_LIMITS_STRICT", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "prod")

    assert prefect_limits._strict_limits() is False


def test_strict_limits_reads_explicit_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operators can fail closed after global limits are bootstrapped."""
    monkeypatch.setenv("PREFECT_GLOBAL_LIMITS_STRICT", "true")
    monkeypatch.setenv("ENVIRONMENT", "prod")

    assert prefect_limits._strict_limits() is True


@pytest.mark.asyncio
async def test_wait_for_provider_api_credit_fails_open_when_not_strict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider calls should continue locally if the Prefect limit is absent."""
    calls: list[dict[str, object]] = []
    policy = ProviderRateLimitPolicy(burst_capacity=2, slot_decay_per_second=1.0)

    async def unavailable_rate_limit(*args: object, **kwargs: object) -> None:
        calls.append({"args": args, "kwargs": kwargs})
        raise RuntimeError("limit missing")

    monkeypatch.delenv("PREFECT_GLOBAL_LIMITS_STRICT", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setattr(prefect_limits, "_provider_api_credit_limits_missing", set())
    monkeypatch.setattr(prefect_limits, "rate_limit", unavailable_rate_limit)

    await prefect_limits.wait_for_provider_api_credit(provider="demo", policy=policy, operation="GET /prices")
    await prefect_limits.wait_for_provider_api_credit(provider="demo", policy=policy, operation="GET /prices")

    assert len(calls) == 1
    assert calls[0]["args"] == ("unique-stocks.provider.demo",)


@pytest.mark.asyncio
async def test_setup_prefect_limits_uses_supplied_lake_limit(capsys: pytest.CaptureFixture[str]) -> None:
    """Dry-run should use supplied lake limit and provider policies."""
    exit_code = await setup_prefect_limits(
        provider_clients=(ClientWithPolicy, ClientWithoutPolicy),
        lake_writer_limit=1,
        dry_run=True,
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "unique-stocks.lake-writer: limit=1" in output
    assert "unique-stocks.provider.eodhd: burst_capacity=100" in output
    assert "slot_decay_per_second=10.0" in output


@pytest.mark.asyncio
async def test_setup_prefect_limits_accepts_higher_supplied_lake_limit(capsys: pytest.CaptureFixture[str]) -> None:
    """Callers can supply a higher lake writer limit."""
    exit_code = await setup_prefect_limits(
        provider_clients=(ClientWithPolicy, ClientWithoutPolicy),
        lake_writer_limit=4,
        dry_run=True,
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "unique-stocks.lake-writer: limit=4" in output


@pytest.mark.asyncio
async def test_setup_prefect_limits_accepts_override_lake_limit(capsys: pytest.CaptureFixture[str]) -> None:
    """Callers can pass environment-derived overrides without core reading settings."""
    exit_code = await setup_prefect_limits(
        provider_clients=(ClientWithPolicy, ClientWithoutPolicy),
        lake_writer_limit=2,
        dry_run=True,
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "unique-stocks.lake-writer: limit=2" in output
    assert "unique-stocks.provider.eodhd: burst_capacity=100" in output
