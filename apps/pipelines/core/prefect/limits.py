"""Prefect global concurrency and rate-limit setup."""

from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any

from prefect.client.orchestration import get_client

from config.settings import DEFAULT_PREFECT_LAKE_WRITER_LIMIT
from core.prefect.controls import LAKE_WRITER_LIMIT
from core.prefect.provider_rate_limits import provider_rate_limit_registrations


async def setup_prefect_limits(
    *,
    providers: Iterable[Any],
    dry_run: bool,
) -> int:
    """Upsert Prefect global limits for shared lake and provider API resources."""
    lake_limit = _env_int("PREFECT_LAKE_WRITER_LIMIT", DEFAULT_PREFECT_LAKE_WRITER_LIMIT)
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


def _global_limit_message(action: str, name: str, limit: int) -> str:
    return f"{action} global limit {name}: limit={limit}"


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)
