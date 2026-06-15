from dataclasses import dataclass, field

from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate


@dataclass
class LimitRegistry:
    """App-owned registry of Prefect limits."""

    limits: list[GlobalConcurrencyLimitCreate] = field(default_factory=list)

    def register(self, limit: GlobalConcurrencyLimitCreate) -> None:
        """Register a new limit."""
        self.limits.append(limit)


LIMIT_REGISTRY = LimitRegistry()


__all__ = ["LIMIT_REGISTRY"]
