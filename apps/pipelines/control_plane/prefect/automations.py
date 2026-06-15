"""Application Prefect event automation definitions."""

from prefect.automations import Automation
from prefect.events.actions import DoNothing
from prefect.events.schemas.automations import EventTrigger, Posture

from core.orchestration.automations import (
    define_automations,
)
from core.prefect.events import PrefectEvent

AUTOMATION_TAGS = ["unique-stocks", "pipelines"]

PREFECT_AUTOMATIONS = define_automations(
    [
        Automation(
            name="unique-stocks dbt failure alert",
            description="Runs when a dbt invocation fails or records failed dbt nodes.",
            tags=AUTOMATION_TAGS,
            trigger=EventTrigger(
                expect={PrefectEvent.DBT_FAILED},
                posture=Posture.Reactive,
                threshold=1,
            ),
            actions=[
                DoNothing(),
            ],
        ),
        Automation(
            name="unique-stocks coverage gate alert",
            description="Runs when the EOD price coverage gate finds gaps.",
            tags=AUTOMATION_TAGS,
            trigger=EventTrigger(
                expect={PrefectEvent.COVERAGE_GATE_FAILED},
                posture=Posture.Reactive,
                threshold=1,
            ),
            actions=[
                DoNothing(),
            ],
        ),
        Automation(
            name="unique-stocks ingestion partial alert",
            description="Runs when an ingestion flow completes with partial data.",
            tags=AUTOMATION_TAGS,
            trigger=EventTrigger(
                expect={PrefectEvent.INGESTION_PARTIAL},
                posture=Posture.Reactive,
                threshold=1,
            ),
            actions=[
                DoNothing(),
            ],
        ),
        Automation(
            name="unique-stocks ingestion failure alert",
            description="Runs when an ingestion flow fails before completing cleanly.",
            tags=AUTOMATION_TAGS,
            trigger=EventTrigger(
                expect={PrefectEvent.INGESTION_FAILED},
                posture=Posture.Reactive,
                threshold=1,
            ),
            actions=[
                DoNothing(),
            ],
        ),
        Automation(
            name="unique-stocks stale running audit alert",
            description="Runs when operational health detects stale running pipeline rows.",
            tags=AUTOMATION_TAGS,
            trigger=EventTrigger(
                expect={PrefectEvent.PIPELINE_STALE_RUNNING},
                posture=Posture.Reactive,
                threshold=1,
            ),
            actions=[
                DoNothing(),
            ],
        ),
        Automation(
            name="unique-stocks cancellation audit alert",
            description="Runs when a pipeline run is cancelled by orchestration.",
            tags=AUTOMATION_TAGS,
            trigger=EventTrigger(
                expect={PrefectEvent.PIPELINE_CANCELLED},
                posture=Posture.Reactive,
                threshold=1,
            ),
            actions=[
                DoNothing(),
            ],
        ),
    ]
)

__all__ = ["PREFECT_AUTOMATIONS"]
