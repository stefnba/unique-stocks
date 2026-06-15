"""Generic Prefect automation registry helpers."""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field

from prefect.automations import Automation
from prefect.client.orchestration import get_client


class CustomAutomation(ABC):
    """Base class for app-owned automation presets.

    Subclasses can centralize local defaults such as tags, trigger posture, and
    shared actions while still compiling to Prefect's native ``Automation``.
    """

    @abstractmethod
    def to_prefect_automation(self) -> Automation:
        """Return the native Prefect automation represented by this preset."""


type AutomationDefinition = Automation | CustomAutomation


@dataclass(frozen=True, slots=True)
class AutomationRegistry:
    """Collection of Prefect automations managed as one explicit app registry.

    The registry stores desired automation definitions only. ``sync`` resolves
    existing server-side automations by stable name, then creates missing
    automations or updates existing automations with the current desired
    definition.
    """

    automations: tuple[Automation, ...] = field(default_factory=tuple)

    async def sync(self, *, plan: bool) -> int:
        """Create or update every automation in this registry.

        Args:
            plan: When true, print planned changes without writing to the Prefect API.

        Returns:
            Process-style exit code ``0`` after all planned or applied changes
            complete successfully.
        """
        for automation in self.automations:
            try:
                existing = await Automation.aread(name=automation.name)
            except ValueError as exc:
                if str(exc) != f"Automation with name {automation.name!r} not found":
                    raise
                existing = None

            # create
            if existing is None:
                if plan:
                    print(f"Would create automation: {automation.name}")
                    continue

                await automation.model_copy(deep=True).acreate()
                print(f"Created automation: {automation.name}")
                continue

            # update
            if plan:
                print(f"Would update automation: {automation.name}")
                continue

            desired = automation.model_copy(deep=True)
            desired.id = existing.id
            await desired.aupdate()
            print(f"Updated automation: {automation.name}")
        return 0

    async def delete_automations(self) -> None:
        """Delete every Prefect automation visible to the configured API.

        This is intentionally broader than this registry's desired automation
        list and is meant for environment cleanup workflows.
        """
        async with get_client() as client:
            existing = {automation.name: automation for automation in await client.read_automations()}
            for automation in existing.values():
                await client.delete_automation(automation.id)
                print(f"Deleted automation: {automation.name}")


def define_automations(automations: Sequence[AutomationDefinition]) -> AutomationRegistry:
    """Create an immutable automation registry from desired Prefect definitions.

    Args:
        automations: Desired Prefect automation definitions owned by the app.

    Returns:
        Registry that can sync those definitions to the configured Prefect API.
    """
    # convert custom automations to native automations
    automations = tuple(
        definition if isinstance(definition, Automation) else definition.to_prefect_automation()
        for definition in automations
    )

    return AutomationRegistry(automations=automations)


def _validate_unique_names(automations: Sequence[Automation]) -> None:
    """Reject duplicate desired automation names before touching Prefect."""
    seen: set[str] = set()
    duplicates: list[str] = []
    for automation in automations:
        if automation.name in seen:
            duplicates.append(automation.name)
        seen.add(automation.name)
    if duplicates:
        names = ", ".join(sorted(set(duplicates)))
        raise ValueError(f"Duplicate Prefect automation name(s): {names}")
