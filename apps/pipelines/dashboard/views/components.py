"""Shared Streamlit UI components for dashboard pages."""

from __future__ import annotations

import os
from typing import Any

import streamlit as st

from dashboard.constants import (
    DBT_NODE_RESULTS_LIMIT,
    LANDING_OBJECTS_LIMIT,
    REJECTIONS_LIMIT,
    RUN_UNITS_LIMIT,
)
from dashboard.formatting import (
    error_text,
    format_int,
    format_key_value,
    format_status,
    humanize_column,
    json_value_parsed,
    jsonable,
)
from dashboard.tables import frame, render_compact_dataframe, render_landing_object_overview_table


def render_limit_note(rows: list[dict[str, Any]], *, limit: int, label: str) -> None:
    """Render a caption when a result set hits a configured fetch limit.

    Args:
        rows: Loaded evidence rows.
        limit: Maximum number of rows fetched from the lake.
        label: Human-readable evidence type such as ``work units``.
    """
    if len(rows) >= limit:
        st.caption(f"Showing first {format_int(limit)} {label}. Narrow the scope if you need the rest.")


def render_run_status_callout(run: dict[str, Any]) -> None:
    """Render a prominent status banner for one pipeline run.

    Args:
        run: Run row from ``pipeline.runs`` including status and error fields.
    """
    status = str(run.get("status") or "").lower()
    error_class = run.get("error_class")
    error_message = run.get("error_message")
    units_failed = int(run.get("units_failed") or 0)
    rows_rejected = int(run.get("rows_rejected") or 0)
    if status == "failed":
        st.error(error_text("Run failed", error_class=error_class, error_message=error_message))
    elif status == "partial":
        st.warning(error_text("Run completed partially", error_class=error_class, error_message=error_message))
    elif status == "running":
        st.info("Run is still running. Stale detection uses the stale-hours threshold on Overview.")
    elif status == "completed":
        if units_failed or rows_rejected:
            st.warning(
                f"Run completed with {format_int(units_failed)} failed unit(s) "
                f"and {format_int(rows_rejected)} rejected row(s)."
            )
        else:
            st.success("Run completed without failed units or parser rejections.")
    elif status == "skipped":
        st.info("Run skipped because there was no eligible work.")


def render_unit_status_callout(unit: dict[str, Any]) -> None:
    """Render a prominent status banner for one work unit.

    Args:
        unit: Work-unit row from ``pipeline.run_units``.
    """
    status = str(unit.get("status") or "").lower()
    error_class = unit.get("error_class")
    error_message = unit.get("error_message")
    if status == "failed":
        st.error(error_text("Unit failed", error_class=error_class, error_message=error_message))
    elif status == "unsupported":
        st.warning(error_text("Unit was unsupported", error_class=error_class, error_message=error_message))
    elif status == "skipped":
        st.info("Unit skipped. Check the reason and unit key before replaying work.")
    elif status == "completed":
        st.success("Unit completed.")


def render_key_fields(value: object) -> None:
    """Render parsed unit-key fields as bordered summary cards.

    Args:
        value: ``unit_key_json`` payload from a work-unit row.
    """
    unit_key = json_value_parsed(value)
    if not isinstance(unit_key, dict) or not unit_key:
        st.info("No structured unit key recorded.")
        return

    items = list(unit_key.items())
    for start in range(0, len(items), 3):
        chunk = items[start : start + 3]
        columns = st.columns(len(chunk))
        for column, (key, item_value) in zip(columns, chunk, strict=False):
            with column.container(border=True):
                st.caption(humanize_column(key))
                st.markdown(f"**{format_key_value(item_value)}**")


def render_landing_table(rows: list[dict[str, Any]], *, key: str) -> None:
    """Render landing-object evidence rows for a run or unit scope.

    Args:
        rows: Landing-object rows from ``pipeline.landing_objects``.
        key: Unique Streamlit widget key for the landing table.
    """
    table = frame(rows)
    if table.empty:
        st.info("No landing objects recorded for this scope.")
        return
    render_landing_object_overview_table(table, key=key)
    render_limit_note(rows, limit=LANDING_OBJECTS_LIMIT, label="landing objects")


def render_rejection_table(rows: list[dict[str, Any]]) -> None:
    """Render sampled parser rejection rows for a run or unit scope.

    Args:
        rows: Rejection rows from ``pipeline.rejections``.
    """
    table = frame(rows)
    if table.empty:
        st.info("No sampled rejections recorded for this scope.")
        return
    render_compact_dataframe(
        table,
        columns=[
            "recorded_at",
            "reason",
            "error_class",
            "error_message",
            "entity_key_json",
            "source_uri",
            "raw_sample_json",
            "unit_id",
        ],
    )
    render_limit_note(rows, limit=REJECTIONS_LIMIT, label="rejection samples")


def render_dbt_table(rows: list[dict[str, Any]]) -> None:
    """Render dbt node result rows linked to a pipeline run.

    Args:
        rows: Joined dbt invocation/node rows for one pipeline run.
    """
    table = frame(rows)
    if table.empty:
        st.info("No dbt node results recorded for this run.")
        return
    render_compact_dataframe(
        table,
        columns=[
            "status",
            "resource_type",
            "unique_id",
            "execution_time",
            "failures",
            "rows_affected",
            "relation_name",
            "message",
            "command",
            "target",
        ],
    )
    render_limit_note(rows, limit=DBT_NODE_RESULTS_LIMIT, label="dbt node results")


def render_run_payload(run: dict[str, Any]) -> None:
    """Render durable run metadata and JSON payloads in expanders.

    Args:
        run: Run row containing metadata and JSON payload columns.
    """
    run_metadata = {
        "run_id": run.get("run_id"),
        "parent_run_id": run.get("parent_run_id"),
        "prefect_flow_run_id": run.get("prefect_flow_run_id"),
        "domain": run.get("domain"),
        "run_kind": run.get("run_kind"),
        "provider": run.get("provider"),
        "environment": run.get("environment"),
        "code_version": run.get("code_version"),
        "target_window_start": run.get("target_window_start"),
        "target_window_end": run.get("target_window_end"),
        "started_at": run.get("started_at"),
        "completed_at": run.get("completed_at"),
        "error_class": run.get("error_class"),
        "error_message": run.get("error_message"),
    }
    with st.expander("Run Metadata", expanded=True):
        st.json(jsonable(run_metadata))
    with st.expander("Parameters"):
        st.json(json_value_parsed(run.get("parameters_json")))
    with st.expander("Summary"):
        st.json(json_value_parsed(run.get("summary_json")))


def render_unit_status_summary(rows: list[dict[str, Any]]) -> None:
    """Render compact unit-status counters above the unit investigation table.

    Args:
        rows: Unit status breakdown rows with ``status`` and ``units`` fields.
    """
    table = frame(rows)
    if table.empty:
        return
    columns = st.columns(min(4, max(1, len(table))))
    for column, row in zip(columns, table.to_dict(orient="records"), strict=False):
        with column:
            st.metric(format_status(row.get("status")), format_int(row.get("units")))


def render_units_limit_note(unit_rows: list[dict[str, Any]]) -> None:
    """Render the work-unit fetch limit note for a run investigation table.

    Args:
        unit_rows: Loaded work-unit rows for one run.
    """
    render_limit_note(unit_rows, limit=RUN_UNITS_LIMIT, label="work units")


def prefect_flow_run_url(prefect_flow_run_id: object) -> str | None:
    """Build a Prefect UI URL for one flow run when configured.

    Args:
        prefect_flow_run_id: Prefect flow-run UUID from the audit row.

    Returns:
        Absolute Prefect UI URL, or ``None`` when the id or base URL is missing.
    """
    if not prefect_flow_run_id:
        return None
    base_url = os.getenv("PREFECT_UI_URL") or os.getenv("PREFECT_UI_API_URL", "").removesuffix("/api")
    if not base_url:
        return None
    return f"{base_url.rstrip('/')}/runs/flow-run/{prefect_flow_run_id}"
