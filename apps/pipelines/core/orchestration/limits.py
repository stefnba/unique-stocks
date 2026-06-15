"""Generic Prefect global limit registry helpers."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate, GlobalConcurrencyLimitUpdate
from prefect.exceptions import ObjectNotFound

from core.global_limits import LAKE_WRITER_LIMIT
from core.http.registry import HttpClientRegistry

type LimitDefinitions = Sequence[GlobalConcurrencyLimitCreate] | Callable[[], Sequence[GlobalConcurrencyLimitCreate]]


@dataclass(frozen=True, slots=True)
class LimitRegistry:
    """Collection of Prefect global limits managed as one explicit app registry.

    The registry stores desired global concurrency limit definitions only.
    ``sync`` resolves existing server-side limits by stable name, then creates
    missing limits or updates existing limits with the current desired
    definition.
    """

    limits: LimitDefinitions = field(default_factory=tuple)

    def _resolve_limits(self) -> tuple[GlobalConcurrencyLimitCreate, ...]:
        """Return the concrete Prefect limit definitions for this sync run.

        Returns:
            Desired global concurrency limit definitions. Callables are resolved
            lazily so app settings can be read at command runtime.
        """
        limits = self.limits() if callable(self.limits) else self.limits
        return tuple(limits)

    async def sync(self, *, plan: bool) -> int:
        """Create or update every global concurrency limit in this registry.

        Args:
            plan: When true, print planned changes without writing to the Prefect API.

        Returns:
            Process-style exit code ``0`` after all planned or applied changes
            complete successfully.
        """
        async with get_client() as client:
            for limit in self._resolve_limits():
                try:
                    await client.read_global_concurrency_limit_by_name(limit.name)
                except ObjectNotFound:
                    exists = False
                else:
                    exists = True

                if not exists:
                    if plan:
                        print(f"Would create global limit {limit.name}: {_limit_summary(limit)}")
                        continue

                    await client.create_global_concurrency_limit(limit)
                    print(f"Created global limit {limit.name}: {_limit_summary(limit)}")
                    continue

                if plan:
                    print(f"Would update global limit {limit.name}: {_limit_summary(limit)}")
                    continue

                await client.update_global_concurrency_limit(
                    limit.name,
                    GlobalConcurrencyLimitUpdate(
                        limit=limit.limit,
                        active=limit.active,
                        slot_decay_per_second=limit.slot_decay_per_second,
                    ),
                )
                print(f"Updated global limit {limit.name}: {_limit_summary(limit)}")
        return 0


def define_limits(
    http_providers: HttpClientRegistry | None = None,
    lake_writer_limit: int | None = None,
    additional_limits: list[GlobalConcurrencyLimitCreate] | None = None,
) -> LimitRegistry:
    """Create a Prefect global limit registry without touching the Prefect API.

    Args:
        http_providers: HTTP provider classes to define rate limits for.
        lake_writer_limit: Desired Prefect global concurrency limit for the shared lake/dbt writer.
        additional_limits: Additional desired Prefect global concurrency limit definitions.

    Returns:
        Registry that can sync those definitions to the configured Prefect API.
    """
    provider_rate_limits = _define_provider_rate_limits(http_providers)
    lake_writer_limit_def = _define_lake_writer_limit(lake_writer_limit)

    return LimitRegistry(limits=[*provider_rate_limits, *lake_writer_limit_def, *(additional_limits or [])])


def _define_lake_writer_limit(limit: int | None) -> tuple[GlobalConcurrencyLimitCreate, ...]:
    """Build the shared lake/dbt writer concurrency limit definition.

    Args:
        limit: Maximum number of concurrent lake/dbt writer slots.

    Returns:
        Prefect global concurrency limit definition for shared lake writes.

    Raises:
        ValueError: If the supplied limit is less than one.
    """
    if limit is None:
        return ()

    if limit < 1:
        raise ValueError("Prefect lake writer limit must be at least 1.")
    return (GlobalConcurrencyLimitCreate(name=LAKE_WRITER_LIMIT, limit=limit),)


def _define_provider_rate_limits(
    provider_clients: HttpClientRegistry | None,
) -> tuple[GlobalConcurrencyLimitCreate, ...]:
    """Build provider API rate-limit definitions from client policies.

    Provider clients without a ``RATE_LIMIT_POLICY`` are ignored. A provider
    rate limit uses Prefect's ``slot_decay_per_second`` field, which Prefect
    requires for ``rate_limit`` usage.
    """
    if provider_clients is None:
        return ()

    limits: list[GlobalConcurrencyLimitCreate] = []
    for client_cls in provider_clients.values():
        policy = client_cls.RATE_LIMIT_POLICY
        if policy is None:
            continue

        provider = str(client_cls.PROVIDER)
        name = policy.limit_name(provider)

        limits.append(
            GlobalConcurrencyLimitCreate(
                name=name,
                limit=policy.burst_capacity,
                slot_decay_per_second=policy.slot_decay_per_second,
            )
        )
    return tuple(limits)


def _limit_summary(limit: GlobalConcurrencyLimitCreate) -> str:
    """Return a summary of a global concurrency limit."""
    return f"limit={limit.limit}, slot_decay_per_second={limit.slot_decay_per_second}, active={limit.active}"
