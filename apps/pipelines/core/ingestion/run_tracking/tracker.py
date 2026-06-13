"""Public import surface for pipeline run tracking.

The implementation is split across focused modules: ``scopes`` for run/unit
context managers, ``writer`` for lake writes, ``status`` for aggregate status
helpers, and ``utils`` for private normalization/runtime helpers.
"""

from core.ingestion.run_tracking.records import (
    LandingObjectRecord,
    RejectionRecord,
    RunCounters,
    RunStatus,
    RunUnitRecord,
    RunUnitTally,
    UnitStatus,
)
from core.ingestion.run_tracking.scopes import PipelineRunScope, PipelineUnitScope
from core.ingestion.run_tracking.status import terminal_status
from core.ingestion.run_tracking.writer import PipelineRunTracker

__all__ = [
    "LandingObjectRecord",
    "PipelineRunTracker",
    "PipelineRunScope",
    "PipelineUnitScope",
    "RejectionRecord",
    "RunCounters",
    "RunUnitTally",
    "RunUnitRecord",
    "RunStatus",
    "UnitStatus",
    "terminal_status",
]
