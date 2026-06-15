from dataclasses import dataclass, field

from prefect.automations import Automation


@dataclass
class AutomationRegistry:
    """App-owned registry of Prefect automations."""

    automations: list[Automation] = field(default_factory=list)

    def register(self, automation: Automation | list[Automation]) -> None:
        """Register a new automation."""
        if isinstance(automation, list):
            self.automations.extend(automation)
        else:
            self.automations.append(automation)


AUTOMATIONS_REGISTRY = AutomationRegistry()


__all__ = ["AUTOMATIONS_REGISTRY"]
