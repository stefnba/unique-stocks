"""Tests for HTTP provider rate-limit policies."""

import pytest

from core.global_limits import PREFECT_GLOBAL_LIMITS_STRICT_ENV_VAR, provider_rate_limit_name
from core.http import rate_limit as http_rate_limit
from core.http.rate_limit import ProviderRateLimitPolicy


def test_provider_rate_limit_policy_builds_default_limit_name() -> None:
    """Provider policies should derive stable Prefect limit names."""
    policy = ProviderRateLimitPolicy(burst_capacity=100, slot_decay_per_second=10.0)

    assert policy.limit_name("EODHD_API") == provider_rate_limit_name("EODHD_API")


def test_provider_rate_limit_policy_allows_explicit_name() -> None:
    """Provider policies can use an explicit shared limit name when needed."""
    policy = ProviderRateLimitPolicy(
        name="unique-stocks.http.provider.custom",
        burst_capacity=5,
        slot_decay_per_second=0.5,
    )

    assert policy.limit_name("demo") == "unique-stocks.http.provider.custom"


def test_provider_rate_limit_policy_rejects_invalid_values() -> None:
    """Provider policies should fail early when configured with invalid values."""
    with pytest.raises(ValueError, match="burst_capacity"):
        ProviderRateLimitPolicy(burst_capacity=0, slot_decay_per_second=1.0)

    with pytest.raises(ValueError, match="slot_decay_per_second"):
        ProviderRateLimitPolicy(burst_capacity=1, slot_decay_per_second=0)


@pytest.mark.asyncio
async def test_wait_for_provider_limit_credit_fails_open_when_not_strict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider calls should continue locally if the Prefect limit is absent."""
    calls: list[dict[str, object]] = []
    policy = ProviderRateLimitPolicy(burst_capacity=2, slot_decay_per_second=1.0)

    async def unavailable_rate_limit(*args: object, **kwargs: object) -> None:
        calls.append({"args": args, "kwargs": kwargs})
        raise RuntimeError("limit missing")

    monkeypatch.delenv(PREFECT_GLOBAL_LIMITS_STRICT_ENV_VAR, raising=False)
    monkeypatch.setattr(http_rate_limit, "_provider_limit_credit_limits_missing", set())
    monkeypatch.setattr(http_rate_limit, "rate_limit", unavailable_rate_limit)

    await http_rate_limit.wait_for_provider_limit_credit(provider="demo", policy=policy, operation="GET /prices")
    await http_rate_limit.wait_for_provider_limit_credit(provider="demo", policy=policy, operation="GET /prices")

    assert len(calls) == 1
    assert calls[0]["args"] == (provider_rate_limit_name("demo"),)
