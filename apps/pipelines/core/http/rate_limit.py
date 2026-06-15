from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog
from prefect.concurrency.asyncio import rate_limit

logger = structlog.get_logger(__name__)
if TYPE_CHECKING:
    pass

_PROVIDER_RATE_LIMIT_PREFIX = "unique-stocks.http.provider"

_provider_limit_credit_limits_missing: set[str] = set()


@dataclass(frozen=True)
class ProviderRateLimitPolicy:
    """Rate limit policy for a provider API.

    Attributes:
        burst_capacity (int): The maximum number of concurrent "slots" (i.e., requests or operations)
            that may be occupied at once. This defines the upper limit for a burst of activity before
            backoff or throttling is enforced. Must be at least 1.
        slot_decay_per_second (float): The sustained refill rate for the policy, in slots/second.
            This represents how quickly occupied "slots" become available again (i.e., how fast
            you can recover from a burst and resume sending requests). Must be greater than 0.
        name (str | None): Optional explicit Prefect global concurrency limit name. If not set,
            a default name will be constructed based on the provider string.

    Example:
        ```python
        RATE_LIMIT_POLICY = ProviderRateLimitPolicy(
            burst_capacity=100,
            slot_decay_per_second=10.0,
        )
        ```

    Declared on a provider HTTP client as ``RATE_LIMIT_POLICY`` and registered cluster-wide
    via ``make prefect-controls``. Prefect applies the limit before each outbound request starts;
    it does not hold a slot for the full HTTP round-trip.

    Tuning for faster API calls:
        1. Edit ``RATE_LIMIT_POLICY`` on the provider client (for example
           ``providers/eodhd/client.py``).
        2. Raise ``burst_capacity`` to allow more request starts in a short burst before
           Prefect throttles. Raise ``slot_decay_per_second`` to increase sustained
           starts/second after a burst.
        3. Re-register: ``make prefect-controls`` against the same ``PREFECT_API_URL`` as workers.
        4. For one flow run, also raise deployment params such as ``batch_size``.

    Stay below the provider's real quota; HTTP 429 handling still applies when the limit is hit.
    """

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


async def wait_for_provider_limit_credit(
    *,
    provider: str,
    policy: ProviderRateLimitPolicy | None,
    operation: str | None = None,
) -> None:
    """Apply the shared provider/API-credit rate limit before outbound calls."""
    if policy is None:
        return

    limit_name = policy.limit_name(provider)
    strict_limits = _strict_limits()
    if limit_name in _provider_limit_credit_limits_missing and not strict_limits:
        return

    try:
        await rate_limit(
            limit_name,
            occupy=1,
            strict=True,
        )
    except Exception as exc:
        if strict_limits:
            raise
        _provider_limit_credit_limits_missing.add(limit_name)
        logger.warning(
            "prefect_provider_limit_credit_unavailable",
            provider=provider,
            operation=operation,
            limit_name=limit_name,
            error=str(exc),
        )


def _strict_limits() -> bool:
    configured = _env_bool("PREFECT_GLOBAL_LIMITS_STRICT")
    return configured is True


def _env_bool(name: str) -> bool | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false, got {raw!r}")
