"""Tests for Prefect event resource helpers."""

from types import SimpleNamespace

import pytest

from core.orchestration.events import resources as event_resources


def test_event_resource_adds_stable_automation_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    """Primary resources should carry labels used by control-plane automations."""
    monkeypatch.setenv("ENVIRONMENT", "prod")

    resource = event_resources.event_resource(
        resource_id="unique-stocks.demo.1",
        resource_name="Demo",
        labels={"unique-stocks.domain": "demo", "ignored": None},
    )

    assert resource == {
        "prefect.resource.id": "unique-stocks.demo.1",
        "prefect.resource.name": "Demo",
        "unique-stocks.app": "unique-stocks",
        "unique-stocks.service": "pipelines",
        "unique-stocks.environment": "prod",
        "unique-stocks.domain": "demo",
    }


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

    monkeypatch.setattr(event_resources.FlowRunContext, "get", classmethod(lambda cls: flow_context))
    monkeypatch.setattr(event_resources.TaskRunContext, "get", classmethod(lambda cls: task_context))

    resources = event_resources.current_prefect_related_resources()

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
