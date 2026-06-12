"""Create Prefect limits and lightweight event automations for the pipeline."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Sequence
from datetime import timedelta
from typing import Any
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.events.actions import DoNothing, SendNotification
from prefect.events.schemas.automations import AutomationCore, EventTrigger, Posture

from core.prefect_controls import (
    COVERAGE_GATE_FAILED_EVENT,
    DBT_FAILED_EVENT,
    INGESTION_FAILED_EVENT,
    INGESTION_PARTIAL_EVENT,
    LAKE_WRITER_LIMIT,
    PIPELINE_CANCELLED_EVENT,
    PIPELINE_STALE_RUNNING_EVENT,
    PROVIDER_API_CREDIT_LIMIT,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the Prefect controls setup CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned Prefect controls without changing the server.",
    )
    return parser


async def setup_prefect_controls(*, dry_run: bool) -> int:
    """Upsert global limits and event automations."""
    lake_limit = _env_int("PREFECT_LAKE_WRITER_LIMIT", 1)
    provider_limit = _env_int("PREFECT_PROVIDER_API_CREDIT_LIMIT", 1)
    provider_decay = _env_float("PREFECT_PROVIDER_API_CREDIT_DECAY_PER_SECOND", 1.0)
    automations = _automations()

    if dry_run:
        print(f"Would upsert global limit {LAKE_WRITER_LIMIT}: limit={lake_limit}")
        print(
            f"Would upsert global limit {PROVIDER_API_CREDIT_LIMIT}: "
            f"limit={provider_limit}, slot_decay_per_second={provider_decay}"
        )
        for automation in automations:
            print(f"Would upsert automation: {automation.name} (on trigger: {_action_summary(automation)})")
        return 0

    async with get_client() as client:
        await client.upsert_global_concurrency_limit_by_name(
            name=LAKE_WRITER_LIMIT,
            limit=lake_limit,
        )
        await client.upsert_global_concurrency_limit_by_name(
            name=PROVIDER_API_CREDIT_LIMIT,
            limit=provider_limit,
            slot_decay_per_second=provider_decay,
        )
        for automation in automations:
            action = await _upsert_automation(client, automation)
            print(f"{action.capitalize()} automation: {automation.name}")

    print(f"Upserted global limit {LAKE_WRITER_LIMIT}: limit={lake_limit}")
    print(
        f"Upserted global limit {PROVIDER_API_CREDIT_LIMIT}: "
        f"limit={provider_limit}, slot_decay_per_second={provider_decay}"
    )
    return 0


async def _upsert_automation(client: Any, automation: AutomationCore) -> str:
    existing = await client.read_automations_by_name(automation.name)
    if existing:
        await client.update_automation(existing[0].id, automation)
        return "updated"
    await client.create_automation(automation)
    return "created"


def _automations() -> list[AutomationCore]:
    return [
        _automation(
            name="unique-stocks dbt failure alert",
            event=DBT_FAILED_EVENT,
            description="Runs when a dbt invocation fails or records failed dbt nodes.",
        ),
        _automation(
            name="unique-stocks coverage gate alert",
            event=COVERAGE_GATE_FAILED_EVENT,
            description="Runs when the EOD price coverage gate finds gaps.",
        ),
        _automation(
            name="unique-stocks ingestion partial alert",
            event=INGESTION_PARTIAL_EVENT,
            description="Runs when an ingestion flow completes with partial data.",
        ),
        _automation(
            name="unique-stocks ingestion failure alert",
            event=INGESTION_FAILED_EVENT,
            description="Runs when an ingestion flow fails before completing cleanly.",
        ),
        _automation(
            name="unique-stocks stale running audit alert",
            event=PIPELINE_STALE_RUNNING_EVENT,
            description="Runs when operational health detects stale running pipeline rows.",
        ),
        _automation(
            name="unique-stocks cancellation audit alert",
            event=PIPELINE_CANCELLED_EVENT,
            description="Runs when a pipeline run is cancelled by orchestration.",
        ),
    ]


def _automation(*, name: str, event: str, description: str) -> AutomationCore:
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


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return float(raw)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint."""
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    return asyncio.run(setup_prefect_controls(dry_run=args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
