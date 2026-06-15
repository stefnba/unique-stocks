"""Tests for Prefect global limit registries."""

import pytest
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.exceptions import ObjectNotFound

from core.global_limits import LAKE_WRITER_LIMIT, provider_rate_limit_name
from core.http.base import HttpClientBase
from core.http.rate_limit import ProviderRateLimitPolicy
from core.orchestration import limits as orchestration_limits
from core.orchestration.limits import define_limits


class FakeProviderClient(HttpClientBase):
    """Minimal provider client with a declared rate-limit policy."""

    PROVIDER = "eodhd"
    BASE_URL = "https://example.invalid"
    RATE_LIMIT_POLICY = ProviderRateLimitPolicy(burst_capacity=100, slot_decay_per_second=10.0)


class FakeClient:
    """Capture Prefect global concurrency limit sync calls."""

    def __init__(self, existing: set[str] | None = None) -> None:
        """Create a fake Prefect client with optional existing limit names."""
        self.existing = existing or set()
        self.created: list[GlobalConcurrencyLimitCreate] = []
        self.updated: list[tuple[str, object]] = []

    async def read_global_concurrency_limit_by_name(self, name: str) -> object:
        """Return an object for existing limits and raise for missing ones."""
        if name not in self.existing:
            raise ObjectNotFound(Exception("missing"))
        return object()

    async def create_global_concurrency_limit(self, limit: GlobalConcurrencyLimitCreate) -> None:
        """Capture a created limit."""
        self.created.append(limit)

    async def update_global_concurrency_limit(self, name: str, limit: object) -> None:
        """Capture an updated limit."""
        self.updated.append((name, limit))


class FakeClientContext:
    """Async context manager for fake Prefect clients."""

    def __init__(self, client: FakeClient) -> None:
        """Store the fake client."""
        self.client = client

    async def __aenter__(self) -> FakeClient:
        """Return the fake client."""
        return self.client

    async def __aexit__(self, *_: object) -> None:
        """Exit without cleanup."""


def test_define_limits_includes_lake_writer_limit() -> None:
    """Lake writer limits should use the canonical name and reject zero slots."""
    limit = define_limits(lake_writer_limit=2)._resolve_limits()[0]

    assert limit.name == LAKE_WRITER_LIMIT
    assert limit.limit == 2

    with pytest.raises(ValueError, match="at least 1"):
        define_limits(lake_writer_limit=0)


def test_define_limits_includes_provider_rate_limits() -> None:
    """Provider policy objects should become Prefect rate-limit definitions."""
    limits = define_limits(http_providers={"eodhd": FakeProviderClient})._resolve_limits()

    assert limits[0].name == provider_rate_limit_name("eodhd")
    assert limits[0].limit == 100
    assert limits[0].slot_decay_per_second == 10.0


@pytest.mark.asyncio
async def test_limit_registry_sync_creates_missing_limits(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Missing limits should be created through Prefect's current client API."""
    client = FakeClient()
    registry = define_limits(lake_writer_limit=2)
    monkeypatch.setattr(orchestration_limits, "get_client", lambda: FakeClientContext(client))

    exit_code = await registry.sync(plan=False)

    assert exit_code == 0
    assert client.created[0].name == LAKE_WRITER_LIMIT
    assert f"Created global limit {LAKE_WRITER_LIMIT}" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_limit_registry_sync_updates_existing_limits(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Existing limits should be updated by stable name."""
    client = FakeClient(existing={LAKE_WRITER_LIMIT})
    registry = define_limits(lake_writer_limit=2)
    monkeypatch.setattr(orchestration_limits, "get_client", lambda: FakeClientContext(client))

    exit_code = await registry.sync(plan=False)

    assert exit_code == 0
    assert client.updated[0][0] == LAKE_WRITER_LIMIT
    assert f"Updated global limit {LAKE_WRITER_LIMIT}" in capsys.readouterr().out
