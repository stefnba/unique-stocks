"""Tests for Prefect event helpers."""

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
    assert events == [
        {
            "event": prefect_events.PrefectEvent.INGESTION_PARTIAL,
            "resource_id": "unique-stocks.ingestion-run.run-1",
            "resource_name": "instrument-refresh",
            "payload": {
                "app_run_id": "run-1",
                "flow_name": "instrument-refresh",
                "domain": "instrument",
                "status": "partial",
                "summary": {"failed": ["US"], "snapshot_date": "2026-05-31"},
            },
        }
    ]
