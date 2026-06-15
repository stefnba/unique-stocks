from prefect.automations import Automation
from prefect.client.orchestration import get_client

# todo side-effect imports — populates the registries
import control_plane.prefect.automations  # noqa: F401
from core.orchestration.automations import AUTOMATIONS_REGISTRY
from core.orchestration.limits import LIMIT_REGISTRY


async def sync_automations(*, dry_run: bool) -> int:
    """Sync Prefect automations with the app-owned registry.

    This function is idempotent. It will update existing automations and create new ones.
    """
    for automation in AUTOMATIONS_REGISTRY.automations:
        try:
            existing = await Automation.aread(name=automation.name)
        except ValueError as exc:
            if str(exc) != f"Automation with name {automation.name!r} not found":
                raise
            existing = None

        # create
        if existing is None:
            if dry_run:
                print(f"Would create automation: {automation.name}")
                continue

            await automation.model_copy(deep=True).acreate()
            print(f"Created automation: {automation.name}")
            continue

        # update
        if dry_run:
            print(f"Would update automation: {automation.name}")
            continue

        desired = automation.model_copy(deep=True)
        desired.id = existing.id
        await desired.aupdate()
        print(f"Updated automation: {automation.name}")
    return 0


async def delete_automations() -> None:
    """Delete all Prefect automations."""
    async with get_client() as client:
        existing = {automation.name: automation for automation in await client.read_automations()}
        for automation in existing.values():
            await client.delete_automation(automation.id)
            print(f"Deleted automation: {automation.name}")


async def sync_limits() -> None:
    """Sync Prefect limits with the app-owned registry.

    This function is idempotent. It will update existing limits and create new ones.
    """
    async with get_client() as client:
        existing = {limit.name: limit for limit in await client.read_global_concurrency_limits()}

        for limit in LIMIT_REGISTRY.limits:
            if limit.name in existing:
                await client.update_global_concurrency_limit(limit.name, limit)
                print(f"  updated limit: {limit.name}")
            else:
                await client.create_global_concurrency_limit(limit)
                print(f"  created limit: {limit.name}")
