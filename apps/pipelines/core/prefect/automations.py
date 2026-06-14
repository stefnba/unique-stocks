"""Generic Prefect event automation setup helpers."""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.events.actions import DoNothing, SendNotification
from prefect.events.schemas.automations import AutomationCore, EventTrigger, Posture

DEFAULT_NOTIFICATION_BLOCK_ID_ENV_VAR = "PREFECT_NOTIFICATION_BLOCK_ID"


@dataclass(frozen=True, slots=True)
class PrefectAutomationDefinition:
    """Declarative Prefect automation definition."""

    name: str
    event: str
    description: str


@dataclass(frozen=True, slots=True)
class PrefectAutomationRegistry:
    """Named Prefect automations that can be built and upserted together."""

    automations: tuple[PrefectAutomationDefinition, ...]
    tags: tuple[str, ...] = ()
    notification_block_id_env_var: str = DEFAULT_NOTIFICATION_BLOCK_ID_ENV_VAR

    def build(self) -> list[AutomationCore]:
        """Build Prefect automation payloads from this registry."""
        block_id = os.getenv(self.notification_block_id_env_var)
        return [
            build_prefect_automation(
                name=automation.name,
                event=automation.event,
                description=automation.description,
                tags=self.tags,
                action=notification_or_noop_action(
                    block_id=block_id,
                    subject=automation.name,
                ),
            )
            for automation in self.automations
        ]

    async def setup(self, *, dry_run: bool) -> int:
        """Upsert every automation in this registry."""
        return await setup_prefect_automations(automations=self.build(), dry_run=dry_run)


def define_automations(
    automations: Sequence[PrefectAutomationDefinition],
    *,
    tags: Sequence[str] = (),
    notification_block_id_env_var: str = DEFAULT_NOTIFICATION_BLOCK_ID_ENV_VAR,
) -> PrefectAutomationRegistry:
    """Create a Prefect automation registry without upserting it."""
    return PrefectAutomationRegistry(
        automations=tuple(automations),
        tags=tuple(tags),
        notification_block_id_env_var=notification_block_id_env_var,
    )


async def setup_prefect_automations(*, automations: Sequence[AutomationCore], dry_run: bool) -> int:
    """Upsert Prefect event automations."""
    if dry_run:
        for automation in automations:
            print(f"Would upsert automation: {automation.name} (on trigger: {_action_summary(automation)})")
        return 0

    async with get_client() as client:
        for automation in automations:
            action = await upsert_prefect_automation(client, automation)
            print(f"{action.capitalize()} automation: {automation.name}")
    return 0


def build_prefect_automation(
    *,
    name: str,
    event: str,
    description: str,
    tags: Sequence[str] = (),
    action: DoNothing | SendNotification | None = None,
) -> AutomationCore:
    """Build one Prefect event automation payload."""
    return AutomationCore(
        name=name,
        description=description,
        tags=list(tags),
        trigger=EventTrigger(
            expect={event},
            match={},
            posture=Posture.Reactive,
            threshold=1,
            within=timedelta(seconds=0),
        ),
        actions=[],
        actions_on_trigger=[action or DoNothing(type="do-nothing")],
    )


def notification_or_noop_action(*, block_id: str | None, subject: str) -> DoNothing | SendNotification:
    """Return a notification action when configured, otherwise a no-op action."""
    block_id = (block_id or "").strip()
    if not block_id:
        return DoNothing(type="do-nothing")
    return SendNotification(
        type="send-notification",
        block_document_id=UUID(block_id),
        subject=subject,
        body=("{{ event.event }} on {{ event.resource['prefect.resource.name'] }}\n\nPayload:\n{{ event.payload }}"),
    )


async def upsert_prefect_automation(client: Any, automation: AutomationCore) -> str:
    """Create or update one Prefect automation and return the action taken."""
    existing = await client.read_automations_by_name(automation.name)
    if existing:
        await client.update_automation(existing[0].id, automation)
        return "updated"
    await client.create_automation(automation)
    return "created"


def _action_summary(automation: AutomationCore) -> str:
    actions = automation.actions_on_trigger or automation.actions
    if not actions:
        return "none"
    return ", ".join(action.type for action in actions)


__all__ = [
    "DEFAULT_NOTIFICATION_BLOCK_ID_ENV_VAR",
    "PrefectAutomationDefinition",
    "PrefectAutomationRegistry",
    "build_prefect_automation",
    "define_automations",
    "notification_or_noop_action",
    "setup_prefect_automations",
    "upsert_prefect_automation",
]
