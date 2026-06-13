"""Tests for Prefect limit setup."""

from collections.abc import Generator
from contextlib import contextmanager

import pytest

from core.prefect import limits as prefect_limits
from core.prefect.limits import (
    ProviderRateLimitPolicy,
    iter_provider_http_clients,
    provider_rate_limit_registrations,
    setup_prefect_limits,
)
from providers.eodhd.client import EODHDClient
from providers.iso10383.client import ISO10383Client
from providers.registry import Provider


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


def test_iter_provider_http_clients_discovers_known_provider_clients() -> None:
    """Provider discovery should find HTTP clients from Provider enum packages."""
    clients = set(iter_provider_http_clients(Provider))

    assert EODHDClient in clients
    assert ISO10383Client in clients


def test_provider_rate_limit_registrations_include_only_declared_policies() -> None:
    """Only clients declaring RATE_LIMIT_POLICY should produce Prefect registrations."""
    registrations = list(provider_rate_limit_registrations(Provider))

    assert [registration.name for registration in registrations] == ["unique-stocks.provider.eodhd"]
    assert registrations[0].burst_capacity == 100
    assert registrations[0].slot_decay_per_second == 10.0


def test_lake_writer_limit_fails_open_when_not_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local bootstrap should continue if Prefect global limits are not ready."""

    @contextmanager
    def unavailable_limit(*_: object, **__: object) -> Generator[None]:
        raise RuntimeError("limit missing")
        yield

    monkeypatch.delenv("PREFECT_GLOBAL_LIMITS_STRICT", raising=False)
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

    with pytest.raises(RuntimeError, match="limit missing"), prefect_limits.lake_writer_limit("test"):
        pass


@pytest.mark.asyncio
async def test_wait_for_provider_api_credit_fails_open_when_not_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provider calls should continue locally if the Prefect limit is absent."""
    calls: list[dict[str, object]] = []
    policy = ProviderRateLimitPolicy(burst_capacity=2, slot_decay_per_second=1.0)

    async def unavailable_rate_limit(*args: object, **kwargs: object) -> None:
        calls.append({"args": args, "kwargs": kwargs})
        raise RuntimeError("limit missing")

    monkeypatch.delenv("PREFECT_GLOBAL_LIMITS_STRICT", raising=False)
    monkeypatch.setattr(prefect_limits, "_provider_api_credit_limits_missing", set())
    monkeypatch.setattr(prefect_limits, "rate_limit", unavailable_rate_limit)

    await prefect_limits.wait_for_provider_api_credit(provider="demo", policy=policy, operation="GET /prices")
    await prefect_limits.wait_for_provider_api_credit(provider="demo", policy=policy, operation="GET /prices")

    assert len(calls) == 1
    assert calls[0]["args"] == ("unique-stocks.provider.demo",)


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
