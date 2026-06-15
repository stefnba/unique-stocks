"""Reusable helpers for local flow-check commands."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import date

import structlog

type FlowCheckSummary = dict[str, object]
type FlowCheckHandler = Callable[["FlowCheckRequest"], Awaitable[FlowCheckSummary]]
type FlowCheckGuard = Callable[[], None]

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class FlowCheckRequest:
    """Explicit request for one local flow-check run."""

    preset: str
    exchange: str | None = None
    instrument: str | None = None
    snapshot_date: date | None = None
    trade_date: date | None = None


@dataclass(frozen=True, slots=True)
class FlowCheckRegistry:
    """Registry that dispatches named local flow-check presets."""

    handlers: Mapping[str, FlowCheckHandler]
    aliases: Mapping[str, str]
    guard: FlowCheckGuard

    def normalize_preset(self, value: str) -> str:
        """Normalize preset aliases and reject unsupported names."""
        normalized = value.strip().lower()
        preset = self.aliases.get(normalized, normalized)
        if preset not in self.handlers:
            raise ValueError(f"Unsupported flow-check preset: {value}")
        return preset

    async def run(self, request: FlowCheckRequest) -> FlowCheckSummary:
        """Run a flow-check preset through its registered handler."""
        self.guard()
        preset = self.normalize_preset(request.preset)
        normalized = replace(request, preset=preset)
        log.info("flow_check.run_start", preset=preset)
        summary = await self.handlers[preset](normalized)
        log.info("flow_check.run_done", preset=preset, summary=summary)
        return summary


def require_request_value(value: str | None, *, field: str, preset: str) -> str:
    """Return a required request value or raise a clear preset error."""
    if not value:
        raise ValueError(f"{field} is required for {preset} flow-check preset")
    return value
