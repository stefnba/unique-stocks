"""Combined Prefect setup helpers."""

from __future__ import annotations

from core.prefect.automations import setup_prefect_automations
from core.prefect.limits import setup_prefect_limits
from providers.registry import Provider


async def setup_prefect_controls(*, dry_run: bool) -> int:
    """Upsert Prefect limits and event automations."""
    limits_exit = await setup_prefect_limits(providers=Provider, dry_run=dry_run)
    automations_exit = await setup_prefect_automations(dry_run=dry_run)
    return max(limits_exit, automations_exit)
