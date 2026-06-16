"""Stable Prefect event contract shared by emitters and automations.

Core owns these names because runtime code emits the events. Control-plane
modules may import this contract to build automations, but core orchestration
code must not depend on control-plane configuration or actions.
"""

from __future__ import annotations

from enum import StrEnum

APP_LABEL = "unique-stocks"
PIPELINES_LABEL = "pipelines"


class PrefectEvent(StrEnum):
    """Canonical Prefect event names emitted and automated by this app."""

    DBT_FAILED = "unique-stocks.dbt.failed"
    COVERAGE_GATE_FAILED = "unique-stocks.coverage-gate.failed"
    INGESTION_PARTIAL = "unique-stocks.ingestion.partial"
    INGESTION_FAILED = "unique-stocks.ingestion.failed"
    PIPELINE_STALE_RUNNING = "unique-stocks.pipeline.stale-running"
    PIPELINE_CANCELLED = "unique-stocks.pipeline.cancelled"


__all__ = ["APP_LABEL", "PIPELINES_LABEL", "PrefectEvent"]
