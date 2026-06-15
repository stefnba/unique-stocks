"""Tests for Prefect event helpers."""

from types import SimpleNamespace

import pytest

from core.orchestration import events as prefect_events


@pytest.mark.asyncio
async def test_publish_prefect_ingestion_summary_creates_artifact_and_partial_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Partial ingestion runs should publish a summary artifact and generic event."""
    artifacts: list[dict[str, object]] = []
    events: list[dict[str, object]] = []

    def create_artifact(**kwargs: object) -> str:
        artifacts.append(kwargs)
        return "artifact-id"

    def emit_event(**kwargs: object) -> None:
        events.append(kwargs)

    monkeypatch.setattr(prefect_events, "get_run_context", lambda: object())
    monkeypatch.setattr(prefect_events, "create_markdown_artifact", create_artifact)
    monkeypatch.setattr(prefect_events, "emit_prefect_event", emit_event)

    await prefect_events.publish_prefect_ingestion_summary(
        flow_name="instrument-refresh",
        domain="instrument",
        app_run_id="run-1",
        status="partial",
        summary={"failed": ["US"], "snapshot_date": "2026-05-31"},
    )

    assert artifacts
    assert artifacts[0]["key"] == "ingestion-instrument-refresh-run-1"
    assert len(events) == 1
    assert events[0]["event"] == prefect_events.PrefectEvent.INGESTION_PARTIAL
    assert events[0]["resource_id"] == "unique-stocks.ingestion-run.run-1"
    assert events[0]["resource_name"] == "instrument-refresh"
    assert events[0]["labels"] == {
        "unique-stocks.resource.kind": "ingestion-run",
        "unique-stocks.app_run_id": "run-1",
        "unique-stocks.flow_name": "instrument-refresh",
        "unique-stocks.domain": "instrument",
        "unique-stocks.status": "partial",
        "unique-stocks.severity": "warning",
    }
    assert events[0]["related"] == [
        {
            "prefect.resource.id": "unique-stocks.pipeline-run.run-1",
            "prefect.resource.name": "instrument-refresh",
            "prefect.resource.role": "app-run",
            "unique-stocks.app_run_id": "run-1",
            "unique-stocks.flow_name": "instrument-refresh",
            "unique-stocks.domain": "instrument",
        }
    ]
    assert events[0]["payload"] == {
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
    }


@pytest.mark.asyncio
async def test_publish_prefect_ingestion_summary_skips_artifact_without_run_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct service calls should not create Prefect artifacts without a run context."""
    artifacts: list[dict[str, object]] = []
    events: list[dict[str, object]] = []

    def create_artifact(**kwargs: object) -> str:
        artifacts.append(kwargs)
        return "artifact-id"

    def emit_event(**kwargs: object) -> None:
        events.append(kwargs)

    monkeypatch.setattr(prefect_events, "create_markdown_artifact", create_artifact)
    monkeypatch.setattr(prefect_events, "emit_prefect_event", emit_event)

    await prefect_events.publish_prefect_ingestion_summary(
        flow_name="instrument-refresh",
        domain="instrument",
        app_run_id="run-1",
        status="partial",
        summary={"failed": ["US"], "snapshot_date": "2026-05-31"},
    )

    assert artifacts == []
    assert events
    payload = events[0]["payload"]
    assert isinstance(payload, dict)
    assert payload["artifact_key"] is None


def test_emit_prefect_event_adds_filterable_resource_labels_and_related(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generic emitter should put automation dimensions on resources."""
    emitted: list[dict[str, object]] = []

    def emit_event(**kwargs: object) -> None:
        emitted.append(kwargs)

    monkeypatch.setenv("ENVIRONMENT", "prod")
    monkeypatch.setattr(
        prefect_events,
        "_current_prefect_related_resources",
        lambda: [
            {
                "prefect.resource.id": "prefect.flow-run.flow-1",
                "prefect.resource.role": "flow-run",
            }
        ],
    )
    monkeypatch.setattr(prefect_events, "emit_event", emit_event)

    prefect_events.emit_prefect_event(
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


def test_emit_prefect_event_defaults_environment_to_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    """Automation match labels should be present even in bare local shells."""
    emitted: list[dict[str, object]] = []

    def emit_event(**kwargs: object) -> None:
        emitted.append(kwargs)

    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setattr(prefect_events, "_current_prefect_related_resources", lambda: [])
    monkeypatch.setattr(prefect_events, "emit_event", emit_event)

    prefect_events.emit_prefect_event(
        event="demo.event",
        resource_id="unique-stocks.demo.1",
        resource_name="Demo",
        payload={},
    )

    resource = emitted[0]["resource"]
    assert isinstance(resource, dict)
    assert resource["unique-stocks.environment"] == "dev"


def test_current_prefect_related_resources_include_run_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prefect run context should produce rich related resources without API calls."""
    flow_run = SimpleNamespace(
        id="flow-run-1",
        name="EOD price refresh",
        flow_id="flow-1",
        deployment_id="deployment-1",
        deployment_name="eod-price-daily",
        work_queue_id="queue-1",
        work_queue_name="default",
        work_pool_name="pipelines",
        tags=["daily", "prices"],
    )
    flow_context = SimpleNamespace(flow_run=flow_run, flow=SimpleNamespace(name="eod-price-refresh"))
    task_run = SimpleNamespace(id="task-run-1", name="extract task", flow_run_id="flow-run-1")
    task_context = SimpleNamespace(task_run=task_run, task=SimpleNamespace(name="extract-eod"))

    monkeypatch.setattr(prefect_events.FlowRunContext, "get", classmethod(lambda cls: flow_context))
    monkeypatch.setattr(prefect_events.TaskRunContext, "get", classmethod(lambda cls: task_context))

    resources = prefect_events._current_prefect_related_resources()

    assert {
        "prefect.resource.id": "prefect.flow-run.flow-run-1",
        "prefect.resource.name": "EOD price refresh",
        "prefect.resource.role": "flow-run",
    } in resources
    assert {
        "prefect.resource.id": "prefect.task-run.task-run-1",
        "prefect.resource.name": "extract-eod",
        "prefect.resource.role": "task-run",
    } in resources
    assert {
        "prefect.resource.id": "prefect.deployment.deployment-1",
        "prefect.resource.name": "eod-price-daily",
        "prefect.resource.role": "deployment",
    } in resources
    assert {
        "prefect.resource.id": "prefect.work-pool.pipelines",
        "prefect.resource.name": "pipelines",
        "prefect.resource.role": "work-pool",
    } in resources
