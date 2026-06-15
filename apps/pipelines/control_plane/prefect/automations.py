"""Application Prefect event automation definitions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final
from uuid import UUID

from prefect.automations import Automation
from prefect.events.actions import ActionTypes, DoNothing, SendNotification
from prefect.events.schemas.automations import EventTrigger, Posture

from config.settings import get_settings
from control_plane.prefect.blocks import BlockRegistry
from core.orchestration.automations import AutomationRegistry, CustomAutomation, define_automations
from core.orchestration.events import APP_LABEL, PIPELINES_LABEL, PrefectEvent

AUTOMATION_TAGS: Final[list[str]] = ["unique-stocks", "pipelines"]
DEFAULT_FOR_EACH: Final[tuple[str, ...]] = ("unique-stocks.app_run_id",)
DBT_FOR_EACH: Final[tuple[str, ...]] = ("unique-stocks.dbt_run_id",)
RESOURCE_FOR_EACH: Final[tuple[str, ...]] = ("prefect.resource.id",)

_NOTIFICATION_BODY = """Event: {{ event.event }}
Resource: {{ event.resource.get("prefect.resource.name", event.resource.get("prefect.resource.id", "unknown")) }}
Environment: {{ event.resource.get("unique-stocks.environment", "unknown") }}
Status: {{ event.resource.get("unique-stocks.status", "unknown") }}
Domain: {{ event.resource.get("unique-stocks.domain", "n/a") }}
App run: {{ event.resource.get("unique-stocks.app_run_id", event.payload.get("app_run_id", "n/a")) }}
Severity: {{ event.resource.get("unique-stocks.severity", "unknown") }}

Payload:
{{ event.payload }}
"""


@dataclass(frozen=True, slots=True)
class PipelineAlertAutomation(CustomAutomation):
    """Unique Stocks event alert with shared Prefect trigger and action policy."""

    name: str
    description: str
    event: PrefectEvent
    match: Mapping[str, str | list[str]]
    subject: str
    for_each: Sequence[str] = DEFAULT_FOR_EACH
    actions: Sequence[ActionTypes] | None = None

    def to_prefect_automation(self) -> Automation:
        """Build the native Prefect automation for this alert."""
        return Automation(
            name=self.name,
            description=self.description,
            tags=AUTOMATION_TAGS,
            trigger=EventTrigger(
                expect={self.event},
                match=dict(self.match),
                for_each=set(self.for_each),
                posture=Posture.Reactive,
                threshold=1,
            ),
            actions=list(self.actions) if self.actions is not None else alert_actions(subject=self.subject),
        )


def event_match(environment: str | None = None) -> dict[str, str | list[str]]:
    """Return the resource-label match shared by all app event automations."""
    return {
        "unique-stocks.app": APP_LABEL,
        "unique-stocks.service": PIPELINES_LABEL,
        "unique-stocks.environment": environment or str(get_settings().environment),
    }


def alert_actions(
    *,
    subject: str = "Unique Stocks pipeline alert",
    block_document_id: UUID | None = None,
) -> list[ActionTypes]:
    """Return notification actions when Slack is configured, otherwise no-op locally."""
    if not BlockRegistry.SLACK_WEBHOOK.enabled:
        return [DoNothing()]
    return [
        SendNotification(
            block_document_id=block_document_id or BlockRegistry.SLACK_WEBHOOK.document_id(),
            subject=subject,
            body=_NOTIFICATION_BODY,
        )
    ]


def build_prefect_automations(
    *,
    actions: Sequence[ActionTypes] | None = None,
    environment: str | None = None,
) -> AutomationRegistry:
    """Build the app's managed Prefect event automations."""
    match = event_match(environment)
    return define_automations(
        [
            PipelineAlertAutomation(
                name="unique-stocks dbt failure alert",
                description="Runs when a dbt invocation fails or records failed dbt nodes.",
                event=PrefectEvent.DBT_FAILED,
                match=match,
                subject="unique-stocks dbt failure",
                for_each=DBT_FOR_EACH,
                actions=actions,
            ),
            PipelineAlertAutomation(
                name="unique-stocks coverage gate alert",
                description="Runs when the EOD price coverage gate finds gaps.",
                event=PrefectEvent.COVERAGE_GATE_FAILED,
                match=match,
                subject="unique-stocks coverage gate failure",
                actions=actions,
            ),
            PipelineAlertAutomation(
                name="unique-stocks ingestion partial alert",
                description="Runs when an ingestion flow completes with partial data.",
                event=PrefectEvent.INGESTION_PARTIAL,
                match=match,
                subject="unique-stocks ingestion partial",
                actions=actions,
            ),
            PipelineAlertAutomation(
                name="unique-stocks ingestion failure alert",
                description="Runs when an ingestion flow fails before completing cleanly.",
                event=PrefectEvent.INGESTION_FAILED,
                match=match,
                subject="unique-stocks ingestion failure",
                actions=actions,
            ),
            PipelineAlertAutomation(
                name="unique-stocks stale running audit alert",
                description="Runs when operational health detects stale running pipeline rows.",
                event=PrefectEvent.PIPELINE_STALE_RUNNING,
                match=match,
                subject="unique-stocks stale running pipeline audit",
                for_each=RESOURCE_FOR_EACH,
                actions=actions,
            ),
            PipelineAlertAutomation(
                name="unique-stocks cancellation audit alert",
                description="Runs when a pipeline run is cancelled by orchestration.",
                event=PrefectEvent.PIPELINE_CANCELLED,
                match=match,
                subject="unique-stocks pipeline cancellation",
                actions=actions,
            ),
        ]
    )


PREFECT_AUTOMATIONS = build_prefect_automations()
"""Managed native Prefect automations for this app."""

__all__ = [
    "PREFECT_AUTOMATIONS",
    "PipelineAlertAutomation",
    "alert_actions",
    "build_prefect_automations",
    "event_match",
]
