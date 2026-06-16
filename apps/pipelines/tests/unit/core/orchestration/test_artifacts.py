"""Tests for Prefect artifact helpers."""

import pytest

from core.orchestration import artifacts


@pytest.mark.asyncio
async def test_create_ingestion_summary_artifact_creates_markdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Summary artifacts should render compact run context as Markdown."""
    created: list[dict[str, object]] = []

    def create_markdown_artifact(**kwargs: object) -> str:
        created.append(kwargs)
        return "artifact-id"

    monkeypatch.setattr(artifacts, "get_run_context", lambda: object())
    monkeypatch.setattr(artifacts, "create_markdown_artifact", create_markdown_artifact)

    artifact_key = await artifacts.create_ingestion_summary_artifact(
        flow_name="instrument-refresh",
        domain="instrument",
        app_run_id="run-1",
        status="partial",
        summary={"failed": ["US"], "snapshot_date": "2026-05-31"},
    )

    assert artifact_key == "ingestion-instrument-refresh-run-1"
    assert created
    assert created[0]["key"] == artifact_key
    assert created[0]["description"] == "instrument-refresh ingestion summary (partial)."
    markdown = created[0]["markdown"]
    assert isinstance(markdown, str)
    assert "# instrument-refresh partial" in markdown
    assert '"snapshot_date": "2026-05-31"' in markdown


@pytest.mark.asyncio
async def test_create_ingestion_summary_artifact_skips_without_run_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct service calls should not try to create Prefect artifacts."""
    created: list[dict[str, object]] = []

    def missing_run_context() -> object:
        raise RuntimeError("missing run context")

    def create_markdown_artifact(**kwargs: object) -> str:
        created.append(kwargs)
        return "artifact-id"

    monkeypatch.setattr(artifacts, "get_run_context", missing_run_context)
    monkeypatch.setattr(artifacts, "create_markdown_artifact", create_markdown_artifact)

    artifact_key = await artifacts.create_ingestion_summary_artifact(
        flow_name="instrument-refresh",
        domain="instrument",
        app_run_id="run-1",
        status="partial",
        summary={"failed": ["US"], "snapshot_date": "2026-05-31"},
    )

    assert artifact_key is None
    assert created == []
