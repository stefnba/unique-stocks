"""Application Prefect control setup.

This module composes generic Prefect setup helpers with this app's concrete
provider registry. It is intentionally outside ``core`` because it imports app
configuration and knows which provider implementations should be registered for
cluster-wide API rate limits.

Keep reusable Prefect primitives in ``core.prefect``. Keep app registries in
``registry`` and compose them here.
"""

from config.settings import get_settings
from core.prefect.automations import setup_prefect_automations
from core.prefect.limits import setup_prefect_limits
from registry.provider_registry import provider_http_clients


async def setup_prefect_controls(*, dry_run: bool) -> int:
    """Upsert Prefect limits and event automations."""
    limits_exit = await setup_prefect_limits_for_app(dry_run=dry_run)
    automations_exit = await setup_prefect_automations(dry_run=dry_run)
    return max(limits_exit, automations_exit)


async def setup_prefect_limits_for_app(*, dry_run: bool) -> int:
    """Upsert Prefect limits using app provider registry and settings defaults."""
    return await setup_prefect_limits(
        provider_clients=provider_http_clients(),
        lake_writer_limit=get_settings().resolved_prefect_lake_writer_limit(),
        dry_run=dry_run,
    )
