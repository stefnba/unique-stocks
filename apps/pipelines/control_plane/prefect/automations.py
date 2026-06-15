"""Application Prefect event automation definitions."""

from collections.abc import Sequence
from dataclasses import dataclass

from prefect.automations import Automation
from prefect.events.actions import ActionTypes, DoNothing, SendNotification
from prefect.events.schemas.automations import EventTrigger, Posture

from control_plane.prefect.blocks import BlockRegistry
from core.orchestration.automations import (
    CustomAutomation,
    define_automations,
)
from core.orchestration.events import PrefectEvent


@dataclass(frozen=True, slots=True)
class PipelineAlertAutomation(CustomAutomation):
    """Unique Stocks event alert with shared Prefect trigger and action policy."""

    name: str
    description: str
    event: PrefectEvent
    actions: Sequence[ActionTypes]

    def to_prefect_automation(self) -> Automation:
        """Build the native Prefect automation for this alert."""
        return Automation(
            name=self.name,
            description=self.description,
            tags=["unique-stocks", "pipelines"],
            trigger=EventTrigger(
                expect={self.event},
                posture=Posture.Reactive,
                threshold=1,
            ),
            actions=list(self.actions),
        )


def alert_actions() -> list[ActionTypes]:
    """Return the configured Prefect automation alert actions."""
    if not BlockRegistry.SLACK_WEBHOOK.enabled:
        return [DoNothing()]
    return [
        SendNotification(
            block_document_id=BlockRegistry.SLACK_WEBHOOK.document_id(),
            subject="Unique Stocks pipeline alert",
            body=(
                "A Unique Stocks pipeline automation fired in Prefect. "
                "Open the automation event payload in Prefect for the affected resource and run details."
            ),
        )
    ]


PREFECT_AUTOMATIONS = define_automations(
    [
        PipelineAlertAutomation(
            name="unique-stocks dbt failure alert",
            description="Runs when a dbt invocation fails or records failed dbt nodes.",
            event=PrefectEvent.DBT_FAILED,
            actions=alert_actions(),
        ),
        PipelineAlertAutomation(
            name="unique-stocks coverage gate alert",
            description="Runs when the EOD price coverage gate finds gaps.",
            event=PrefectEvent.COVERAGE_GATE_FAILED,
            actions=alert_actions(),
        ),
        PipelineAlertAutomation(
            name="unique-stocks ingestion partial alert",
            description="Runs when an ingestion flow completes with partial data.",
            event=PrefectEvent.INGESTION_PARTIAL,
            actions=alert_actions(),
        ),
        PipelineAlertAutomation(
            name="unique-stocks ingestion failure alert",
            description="Runs when an ingestion flow fails before completing cleanly.",
            event=PrefectEvent.INGESTION_FAILED,
            actions=alert_actions(),
        ),
        PipelineAlertAutomation(
            name="unique-stocks stale running audit alert",
            description="Runs when operational health detects stale running pipeline rows.",
            event=PrefectEvent.PIPELINE_STALE_RUNNING,
            actions=alert_actions(),
        ),
        PipelineAlertAutomation(
            name="unique-stocks cancellation audit alert",
            description="Runs when a pipeline run is cancelled by orchestration.",
            event=PrefectEvent.PIPELINE_CANCELLED,
            actions=alert_actions(),
        ),
    ]
)
"""Managed native Prefect automations for this app."""


__all__ = ["PREFECT_AUTOMATIONS"]
