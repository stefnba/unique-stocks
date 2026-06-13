"""Durable audit API for ingestion run tracking.

This package is the public core surface for recording pipeline run metadata in
the lake. ``records`` contains small typed value objects, ``scopes`` contains
the run/unit context managers, ``writer`` contains low-level lake writes, and
``status`` contains aggregate run-status helpers.
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
    "PipelineRunScope",
    "PipelineRunTracker",
    "PipelineUnitScope",
    "RejectionRecord",
    "RunCounters",
    "RunStatus",
    "RunUnitRecord",
    "RunUnitTally",
    "UnitStatus",
    "terminal_status",
]
