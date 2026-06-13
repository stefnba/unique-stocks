"""Prefect global limit policies, runtime guards, and setup."""

from __future__ import annotations

import inspect
import os
import sys
from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog
from prefect.client.orchestration import get_client
from prefect.concurrency.asyncio import rate_limit
from prefect.concurrency.sync import concurrency

from config.settings import get_settings

if TYPE_CHECKING:
    from core.clients.http.base import HttpClientBase

logger = structlog.get_logger(__name__)

LAKE_WRITER_LIMIT = "unique-stocks.lake-writer"
_PROVIDER_RATE_LIMIT_PREFIX = "unique-stocks.provider"

_lake_writer_limit_missing = False
_provider_api_credit_limits_missing: set[str] = set()


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

    def registration(self, *, provider: str) -> ProviderRateLimitRegistration:
        """Build a concrete Prefect registration for this policy and provider."""
        return ProviderRateLimitRegistration(
            provider=provider,
            name=self.limit_name(provider),
            burst_capacity=self.burst_capacity,
            slot_decay_per_second=self.slot_decay_per_second,
        )

    async def register(self, client: Any, *, provider: str) -> ProviderRateLimitRegistration:
        """Upsert this policy into Prefect."""
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


@contextmanager
def lake_writer_limit(operation: str | None = None) -> Generator[None]:
    """Serialize writes to the shared lake when Prefect limits are configured.

    The default is fail-open so local development and first-run bootstrap do not
    break if the Prefect server has not had limits created yet. Set
    PREFECT_GLOBAL_LIMITS_STRICT=true only after limits are managed.
    """
    global _lake_writer_limit_missing

    strict_limits = _strict_limits()
    if _lake_writer_limit_missing and not strict_limits:
        yield
        return

    manager = concurrency(
        LAKE_WRITER_LIMIT,
        occupy=1,
        strict=True,
        raise_on_lease_renewal_failure=False,
    )
    try:
        manager.__enter__()
    except Exception as exc:
        if strict_limits:
            raise
        _lake_writer_limit_missing = True
        logger.warning(
            "prefect_lake_writer_limit_unavailable",
            operation=operation,
            limit_name=LAKE_WRITER_LIMIT,
            error=str(exc),
        )
        yield
        return

    try:
        yield
    except BaseException:
        exc_type, exc, traceback = sys.exc_info()
        manager.__exit__(exc_type, exc, traceback)
        raise
    else:
        manager.__exit__(None, None, None)


async def wait_for_provider_api_credit(
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
    if limit_name in _provider_api_credit_limits_missing and not strict_limits:
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
        _provider_api_credit_limits_missing.add(limit_name)
        logger.warning(
            "prefect_provider_api_limit_unavailable",
            provider=provider,
            operation=operation,
            limit_name=limit_name,
            error=str(exc),
        )


def iter_provider_http_clients(
    providers: Iterable[Any],
    *,
    package: str = "providers",
) -> Iterator[type[HttpClientBase]]:
    """Yield HTTP client classes discovered from provider package names."""
    from importlib import import_module

    from core.clients.http.base import HttpClientBase

    for provider in providers:
        provider_key = str(provider)
        module = import_module(f"{package}.{provider_key}.client")
        for value in vars(module).values():
            if not inspect.isclass(value):
                continue
            if value is HttpClientBase or not issubclass(value, HttpClientBase):
                continue
            if str(value.PROVIDER) != provider_key:
                continue
            yield value


def provider_rate_limit_registrations(
    providers: Iterable[Any],
    *,
    package: str = "providers",
) -> Iterator[ProviderRateLimitRegistration]:
    """Yield Prefect rate-limit registrations declared by provider clients."""
    for client_cls in iter_provider_http_clients(providers, package=package):
        policy = getattr(client_cls, "RATE_LIMIT_POLICY", None)
        if policy is None:
            continue
        yield policy.registration(provider=str(client_cls.PROVIDER))


async def setup_prefect_limits(
    *,
    providers: Iterable[Any],
    dry_run: bool,
) -> int:
    """Upsert Prefect global limits for shared lake and provider API resources."""
    lake_limit = get_settings().resolved_prefect_lake_writer_limit()
    provider_limits = list(provider_rate_limit_registrations(providers))

    if dry_run:
        print(_global_limit_message("Would upsert", LAKE_WRITER_LIMIT, lake_limit))
        for provider_limit in provider_limits:
            print(
                f"Would upsert provider rate limit {provider_limit.name}: "
                f"burst_capacity={provider_limit.burst_capacity}, "
                f"slot_decay_per_second={provider_limit.slot_decay_per_second}"
            )
        return 0

    async with get_client() as client:
        await client.upsert_global_concurrency_limit_by_name(
            name=LAKE_WRITER_LIMIT,
            limit=lake_limit,
        )
        for provider_limit in provider_limits:
            await provider_limit.upsert(client)

    print(_global_limit_message("Upserted", LAKE_WRITER_LIMIT, lake_limit))
    for provider_limit in provider_limits:
        print(
            f"Upserted provider rate limit {provider_limit.name}: "
            f"burst_capacity={provider_limit.burst_capacity}, "
            f"slot_decay_per_second={provider_limit.slot_decay_per_second}"
        )
    return 0


def _strict_limits() -> bool:
    configured = _env_bool("PREFECT_GLOBAL_LIMITS_STRICT")
    return configured is True


def _global_limit_message(action: str, name: str, limit: int) -> str:
    return f"{action} global limit {name}: limit={limit}"


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
