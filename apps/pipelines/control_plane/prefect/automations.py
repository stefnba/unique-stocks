"""Application Prefect event automation definitions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from prefect.automations import Automation
from prefect.events.actions import ActionTypes, DoNothing, SendNotification
from prefect.events.schemas.automations import EventTrigger, Posture

from config.settings import get_settings
from control_plane.prefect.blocks import BlockRegistry
from core.orchestration.automations import CustomAutomation, define_automations
from core.orchestration.events import APP_LABEL, PIPELINES_LABEL, PrefectEvent


@dataclass(frozen=True, slots=True)
class PipelineAlertAutomation(CustomAutomation):
    """Unique Stocks event alert with shared Prefect trigger and action policy."""

    name: str
    description: str
    event: PrefectEvent
    subject: str
    for_each: Sequence[str]

    def to_prefect_automation(self) -> Automation:
        """Build the native Prefect automation for this alert."""
        return Automation(
            name=self.name,
            description=self.description,
            tags=["unique-stocks", "pipelines"],
            trigger=EventTrigger(
                expect={self.event},
                match={
                    "unique-stocks.app": APP_LABEL,
                    "unique-stocks.service": PIPELINES_LABEL,
                    "unique-stocks.environment": str(get_settings().environment),
                },
                for_each=set(self.for_each),
                posture=Posture.Reactive,
                threshold=1,
            ),
            actions=_alert_actions(self.subject),
        )


def _alert_actions(subject: str) -> list[ActionTypes]:
    if not BlockRegistry.SLACK_WEBHOOK.enabled:
        return [DoNothing()]

    body = "\n".join(
        [
            "Event: {{ event.event }}",
            'Resource: {{ event.resource.get("prefect.resource.name", '
            'event.resource.get("prefect.resource.id", "unknown")) }}',
            'Environment: {{ event.resource.get("unique-stocks.environment", "unknown") }}',
            'Status: {{ event.resource.get("unique-stocks.status", "unknown") }}',
            'Domain: {{ event.resource.get("unique-stocks.domain", "n/a") }}',
            'App run: {{ event.resource.get("unique-stocks.app_run_id", event.payload.get("app_run_id", "n/a")) }}',
            'Severity: {{ event.resource.get("unique-stocks.severity", "unknown") }}',
            "",
            "Payload:",
            "{{ event.payload }}",
            "",
        ]
    )

    return [
        SendNotification(
            block_document_id=BlockRegistry.SLACK_WEBHOOK.document_id(),
            subject=subject,
            body=body,
        )
    ]


PREFECT_AUTOMATIONS = define_automations(
    [
        PipelineAlertAutomation(
            name="unique-stocks dbt failure alert",
            description="Runs when a dbt invocation fails or records failed dbt nodes.",
            event=PrefectEvent.DBT_FAILED,
            subject="unique-stocks dbt failure",
            for_each=("unique-stocks.dbt_run_id",),
        ),
        PipelineAlertAutomation(
            name="unique-stocks coverage gate alert",
            description="Runs when the EOD price coverage gate finds gaps.",
            event=PrefectEvent.COVERAGE_GATE_FAILED,
            subject="unique-stocks coverage gate failure",
            for_each=("unique-stocks.app_run_id",),
        ),
        PipelineAlertAutomation(
            name="unique-stocks ingestion partial alert",
            description="Runs when an ingestion flow completes with partial data.",
            event=PrefectEvent.INGESTION_PARTIAL,
            subject="unique-stocks ingestion partial",
            for_each=("unique-stocks.app_run_id",),
        ),
        PipelineAlertAutomation(
            name="unique-stocks ingestion failure alert",
            description="Runs when an ingestion flow fails before completing cleanly.",
            event=PrefectEvent.INGESTION_FAILED,
            subject="unique-stocks ingestion failure",
            for_each=("unique-stocks.app_run_id",),
        ),
        PipelineAlertAutomation(
            name="unique-stocks stale running audit alert",
            description="Runs when operational health detects stale running pipeline rows.",
            event=PrefectEvent.PIPELINE_STALE_RUNNING,
            subject="unique-stocks stale running pipeline audit",
            for_each=("prefect.resource.id",),
        ),
        PipelineAlertAutomation(
            name="unique-stocks cancellation audit alert",
            description="Runs when a pipeline run is cancelled by orchestration.",
            event=PrefectEvent.PIPELINE_CANCELLED,
            subject="unique-stocks pipeline cancellation",
            for_each=("unique-stocks.app_run_id",),
        ),
    ]
)
"""Managed native Prefect automations for this app."""

__all__ = ["PREFECT_AUTOMATIONS"]
