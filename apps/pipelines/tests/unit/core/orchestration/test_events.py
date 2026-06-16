"""Tests for Prefect event emitters."""

import pytest

from core.orchestration import events as prefect_events
from core.orchestration.events import emitters as event_emitters
from core.orchestration.events import resources as event_resources
from core.orchestration.events.contracts import PrefectEvent


@pytest.mark.asyncio
async def test_publish_ingestion_summary_creates_artifact_and_partial_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The summary publisher should compose artifact creation and event emission."""
    artifacts: list[dict[str, object]] = []
    emitted: list[dict[str, object]] = []

    async def create_artifact(**kwargs: object) -> str:
        artifacts.append(kwargs)
        return "ingestion-instrument-refresh-run-1"

    def emit_status(**kwargs: object) -> None:
        emitted.append(kwargs)

    monkeypatch.setattr(event_emitters, "create_ingestion_summary_artifact", create_artifact)
    monkeypatch.setattr(event_emitters, "emit_ingestion_status", emit_status)

    await prefect_events.publish_ingestion_summary(
        flow_name="instrument-refresh",
        domain="instrument",
        app_run_id="run-1",
        status="partial",
        summary={"failed": ["US"], "snapshot_date": "2026-05-31"},
    )

    assert artifacts == [
        {
            "flow_name": "instrument-refresh",
            "domain": "instrument",
            "app_run_id": "run-1",
            "status": "partial",
            "summary": {"failed": ["US"], "snapshot_date": "2026-05-31"},
        }
    ]
    assert emitted == [
        {
            "flow_name": "instrument-refresh",
            "domain": "instrument",
            "app_run_id": "run-1",
            "status": "partial",
            "summary": {"failed": ["US"], "snapshot_date": "2026-05-31"},
            "artifact_key": "ingestion-instrument-refresh-run-1",
        }
    ]


def test_emit_ingestion_status_shapes_partial_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """Partial ingestion runs should emit the canonical event contract."""
    events: list[dict[str, object]] = []

    def emit_event(**kwargs: object) -> None:
        events.append(kwargs)

    monkeypatch.setattr(event_emitters, "emit_event", emit_event)

    prefect_events.emit_ingestion_status(
        flow_name="instrument-refresh",
        domain="instrument",
        app_run_id="run-1",
        status="partial",
        artifact_key="ingestion-instrument-refresh-run-1",
        summary={"failed": ["US"], "snapshot_date": "2026-05-31"},
    )

    assert events == [
        {
            "event": PrefectEvent.INGESTION_PARTIAL,
            "resource_id": "unique-stocks.ingestion-run.run-1",
            "resource_name": "instrument-refresh",
            "labels": {
                "unique-stocks.resource.kind": "ingestion-run",
                "unique-stocks.app_run_id": "run-1",
                "unique-stocks.flow_name": "instrument-refresh",
                "unique-stocks.domain": "instrument",
                "unique-stocks.status": "partial",
                "unique-stocks.severity": "warning",
            },
            "related": [
                {
                    "prefect.resource.id": "unique-stocks.pipeline-run.run-1",
                    "prefect.resource.name": "instrument-refresh",
                    "prefect.resource.role": "app-run",
                    "unique-stocks.app_run_id": "run-1",
                    "unique-stocks.flow_name": "instrument-refresh",
                    "unique-stocks.domain": "instrument",
                }
            ],
            "payload": {
                "app_run_id": "run-1",
                "flow_name": "instrument-refresh",
                "domain": "instrument",
                "status": "partial",
                "artifact_key": "ingestion-instrument-refresh-run-1",
                "summary": {
                    "failed_count": 1,
                    "failed_sample": ["US"],
                    "failed_truncated": False,
                    "snapshot_date": "2026-05-31",
                },
                "summary_source": "pipeline.runs.summary_json",
            },
        }
    ]


def test_emit_event_adds_filterable_resource_labels_and_related(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generic emitter should put automation dimensions on resources."""
    emitted: list[dict[str, object]] = []

    def emit_event(**kwargs: object) -> None:
        emitted.append(kwargs)

    monkeypatch.setenv("ENVIRONMENT", "prod")
    monkeypatch.setattr(
        event_resources,
        "current_prefect_related_resources",
        lambda: [
            {
                "prefect.resource.id": "prefect.flow-run.flow-1",
                "prefect.resource.role": "flow-run",
            }
        ],
    )
    monkeypatch.setattr(event_emitters, "_emit_prefect_event", emit_event)

    prefect_events.emit_event(
        event="demo.event",
        resource_id="unique-stocks.demo.1",
        resource_name="Demo",
        labels={"unique-stocks.domain": "demo", "ignored": None},
        related=[
            {
                "prefect.resource.id": "unique-stocks.pipeline-run.run-1",
                "prefect.resource.role": "app-run",
            }
        ],
        payload={"when": "now"},
    )

    assert emitted == [
        {
            "event": "demo.event",
            "resource": {
                "prefect.resource.id": "unique-stocks.demo.1",
                "prefect.resource.name": "Demo",
                "unique-stocks.app": "unique-stocks",
                "unique-stocks.service": "pipelines",
                "unique-stocks.environment": "prod",
                "unique-stocks.domain": "demo",
            },
            "related": [
                {
                    "prefect.resource.id": "unique-stocks.pipeline-run.run-1",
                    "prefect.resource.role": "app-run",
                },
                {
                    "prefect.resource.id": "prefect.flow-run.flow-1",
                    "prefect.resource.role": "flow-run",
                },
            ],
            "payload": {"when": "now"},
        }
    ]


def test_emit_event_defaults_environment_to_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    """Automation match labels should be present even in bare local shells."""
    emitted: list[dict[str, object]] = []

    def emit_event(**kwargs: object) -> None:
        emitted.append(kwargs)

    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setattr(event_resources, "current_prefect_related_resources", lambda: [])
    monkeypatch.setattr(event_emitters, "_emit_prefect_event", emit_event)

    prefect_events.emit_event(
        event="demo.event",
        resource_id="unique-stocks.demo.1",
        resource_name="Demo",
        payload={},
    )

    resource = emitted[0]["resource"]
    assert isinstance(resource, dict)
    assert resource["unique-stocks.environment"] == "dev"
