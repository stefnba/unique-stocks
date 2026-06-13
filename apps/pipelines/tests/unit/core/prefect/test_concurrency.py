"""Tests for Prefect concurrency policy helpers."""

import pytest

from core.prefect.concurrency import ProviderRateLimitPolicy


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
