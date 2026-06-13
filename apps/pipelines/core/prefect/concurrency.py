"""Prefect concurrency and rate-limit policy helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_PROVIDER_RATE_LIMIT_PREFIX = "unique-stocks.provider"


@dataclass(frozen=True)
class ProviderRateLimitPolicy:
    """Rate limit policy for a provider API."""

    burst_capacity: int
    """Burst capacity: maximum occupied rate-limit slots at once."""

    slot_decay_per_second: float
    """Sustained refill rate: occupied slots released per second."""

    name: str | None = None
    """Optional explicit Prefect global concurrency limit name."""

    def __post_init__(self) -> None:
        """Validate values early so bad provider policies fail at import/setup time."""
        if self.burst_capacity < 1:
            raise ValueError("Provider rate-limit policy burst_capacity must be at least 1.")
        if self.slot_decay_per_second <= 0:
            raise ValueError("Provider rate-limit policy slot_decay_per_second must be greater than 0.")

    def limit_name(self, provider: str) -> str:
        """Return the Prefect global concurrency limit name for this provider."""
        if self.name:
            return self.name
        provider_key = provider.strip().lower().replace("_", "-")
        return f"{_PROVIDER_RATE_LIMIT_PREFIX}.{provider_key}"

    def registration(self, *, provider: str) -> ProviderRateLimitRegistration:
        """Build a concrete Prefect registration for this policy and provider."""
        return ProviderRateLimitRegistration(
            provider=provider,
            name=self.limit_name(provider),
            burst_capacity=self.burst_capacity,
            slot_decay_per_second=self.slot_decay_per_second,
        )

    async def register(self, client: Any, *, provider: str) -> ProviderRateLimitRegistration:
        """Upsert this policy into Prefect.

        This is intended for setup/sync paths, not per-request HTTP paths.
        """
        registration = self.registration(provider=provider)
        await registration.upsert(client)
        return registration


@dataclass(frozen=True)
class ProviderRateLimitRegistration:
    """Resolved Prefect global concurrency limit for a provider rate policy."""

    provider: str
    name: str
    burst_capacity: int
    slot_decay_per_second: float

    async def upsert(self, client: Any) -> None:
        """Create or update this Prefect global concurrency limit."""
        await client.upsert_global_concurrency_limit_by_name(
            name=self.name,
            limit=self.burst_capacity,
            slot_decay_per_second=self.slot_decay_per_second,
        )
