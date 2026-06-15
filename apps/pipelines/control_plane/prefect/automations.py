"""Application Prefect event automation definitions."""

from prefect.automations import Automation
from prefect.events.actions import DoNothing
from prefect.events.schemas.automations import EventTrigger, Posture

from core.orchestration.automations import (
    define_automations,
)
from core.prefect.events import PrefectEvent

PREFECT_AUTOMATIONS = define_automations(
    [
        Automation(
            name="my-test",
            description="Runs when a dbt invocation fails or records failed dbt nodes.",
            trigger=EventTrigger(
                expect={PrefectEvent.DBT_FAILED},
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

# PREFECT_AUTOMATIONS = define_automations(
#     tags=("unique-stocks", "pipelines"),
#     automations=[
#         PrefectAutomationDefinition(
#             name="unique-stocks dbt failure alert",
#             event=PrefectEvent.DBT_FAILED,
#             description="Runs when a dbt invocation fails or records failed dbt nodes.",
#         ),
#         PrefectAutomationDefinition(
#             name="unique-stocks coverage gate alert",
#             event=PrefectEvent.COVERAGE_GATE_FAILED,
#             description="Runs when the EOD price coverage gate finds gaps.",
#         ),
#         PrefectAutomationDefinition(
#             name="unique-stocks ingestion partial alert",
#             event=PrefectEvent.INGESTION_PARTIAL,
#             description="Runs when an ingestion flow completes with partial data.",
#         ),
#         PrefectAutomationDefinition(
#             name="unique-stocks ingestion failure alert",
#             event=PrefectEvent.INGESTION_FAILED,
#             description="Runs when an ingestion flow fails before completing cleanly.",
#         ),
#         PrefectAutomationDefinition(
#             name="unique-stocks stale running audit alert",
#             event=PrefectEvent.PIPELINE_STALE_RUNNING,
#             description="Runs when operational health detects stale running pipeline rows.",
#         ),
#         PrefectAutomationDefinition(
#             name="unique-stocks cancellation audit alert",
#             event=PrefectEvent.PIPELINE_CANCELLED,
#             description="Runs when a pipeline run is cancelled by orchestration.",
#         ),
#     ],
# )


# __all__ = ["PREFECT_AUTOMATIONS"]
