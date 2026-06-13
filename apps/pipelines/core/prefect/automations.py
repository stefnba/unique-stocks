"""Prefect event automation setup."""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.events.actions import DoNothing, SendNotification
from prefect.events.schemas.automations import AutomationCore, EventTrigger, Posture

from core.prefect.events import (
    COVERAGE_GATE_FAILED_EVENT,
    DBT_FAILED_EVENT,
    INGESTION_FAILED_EVENT,
    INGESTION_PARTIAL_EVENT,
    PIPELINE_CANCELLED_EVENT,
    PIPELINE_STALE_RUNNING_EVENT,
)


async def setup_prefect_automations(*, dry_run: bool) -> int:
    """Upsert Prefect event automations for pipeline operational events."""
    automations = build_prefect_automations()

    if dry_run:
        for automation in automations:
            print(f"Would upsert automation: {automation.name} (on trigger: {_action_summary(automation)})")
        return 0

    async with get_client() as client:
        for automation in automations:
            action = await upsert_prefect_automation(client, automation)
            print(f"{action.capitalize()} automation: {automation.name}")
    return 0


async def upsert_prefect_automation(client: Any, automation: AutomationCore) -> str:
    """Create or update one Prefect automation and return the action taken."""
    existing = await client.read_automations_by_name(automation.name)
    if existing:
        await client.update_automation(existing[0].id, automation)
        return "updated"
    await client.create_automation(automation)
    return "created"


def build_prefect_automations() -> list[AutomationCore]:
    """Return all pipeline Prefect automations managed by setup."""
    return [
        build_prefect_automation(
            name="unique-stocks dbt failure alert",
            event=DBT_FAILED_EVENT,
            description="Runs when a dbt invocation fails or records failed dbt nodes.",
        ),
        build_prefect_automation(
            name="unique-stocks coverage gate alert",
            event=COVERAGE_GATE_FAILED_EVENT,
            description="Runs when the EOD price coverage gate finds gaps.",
        ),
        build_prefect_automation(
            name="unique-stocks ingestion partial alert",
            event=INGESTION_PARTIAL_EVENT,
            description="Runs when an ingestion flow completes with partial data.",
        ),
        build_prefect_automation(
            name="unique-stocks ingestion failure alert",
            event=INGESTION_FAILED_EVENT,
            description="Runs when an ingestion flow fails before completing cleanly.",
        ),
        build_prefect_automation(
            name="unique-stocks stale running audit alert",
            event=PIPELINE_STALE_RUNNING_EVENT,
            description="Runs when operational health detects stale running pipeline rows.",
        ),
        build_prefect_automation(
            name="unique-stocks cancellation audit alert",
            event=PIPELINE_CANCELLED_EVENT,
            description="Runs when a pipeline run is cancelled by orchestration.",
        ),
    ]


def build_prefect_automation(*, name: str, event: str, description: str) -> AutomationCore:
    """Build one Prefect event automation payload."""
    action = _automation_action(name=name)
    return AutomationCore(
        name=name,
        description=description,
        tags=["unique-stocks", "pipelines"],
        trigger=EventTrigger(
            expect={event},
            match={},
            posture=Posture.Reactive,
            threshold=1,
            within=timedelta(seconds=0),
        ),
        actions=[],
        actions_on_trigger=[action],
    )


def _automation_action(*, name: str) -> DoNothing | SendNotification:
    block_id = os.getenv("PREFECT_NOTIFICATION_BLOCK_ID", "").strip()
    if not block_id:
        return DoNothing(type="do-nothing")
    return SendNotification(
        type="send-notification",
        block_document_id=UUID(block_id),
        subject=name,
        body=("{{ event.event }} on {{ event.resource['prefect.resource.name'] }}\n\nPayload:\n{{ event.payload }}"),
    )


def _action_summary(automation: AutomationCore) -> str:
    actions = automation.actions_on_trigger or automation.actions
    if not actions:
        return "none"
    return ", ".join(action.type for action in actions)
