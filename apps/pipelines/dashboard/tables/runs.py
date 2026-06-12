"""Run table shaping and rendering helpers."""

from __future__ import annotations

from typing import Any, cast

import pandas as pd
import streamlit as st

from dashboard.constants import LINK_COLUMN_LABEL_PATTERN
from dashboard.formatting import format_datetime, format_duration, short_id, truncate_text
from dashboard.routing import labeled_href, run_detail_href
from dashboard.tables.base import (
    _TABLE_ROW_HEIGHT,
    _copy_available_columns,
    styled_table,
    table_column_config,
    table_height,
)


def merge_action_queue(
    stale_runs: list[dict[str, Any]],
    attention_runs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge stale and attention runs into one triage list without duplicates.

    Args:
        stale_runs: Runs past the stale threshold while still ``running``.
        attention_runs: Failed or partial runs in the selected window.

    Returns:
        Rows with a ``queue`` field of ``stale`` or ``attention``.
    """
    stale_ids = {str(row.get("run_id")) for row in stale_runs if row.get("run_id")}
    rows: list[dict[str, Any]] = [{**row, "queue": "stale"} for row in stale_runs]
    for row in attention_runs:
        run_id = str(row.get("run_id") or "")
        if run_id and run_id in stale_ids:
            continue
        rows.append({**row, "queue": "attention"})
    return rows


def compact_run_frame(source: pd.DataFrame) -> pd.DataFrame:
    """Build the compact run table shown on overview and triage pages.

    Args:
        source: Raw run rows including ``run_id`` and timing fields.

    Returns:
        Display-ready dataframe with formatted timestamps and truncated errors.
    """
    columns = [
        "queue",
        "run_id_short",
        "status",
        "domain",
        "flow_name",
        "run_kind",
        "started_at",
        "completed_at",
        "duration_seconds",
        "units_total",
        "units_failed",
        "rows_written",
        "rows_rejected",
        "error_class",
        "error_message",
    ]
    result = _copy_available_columns(source, columns)
    if "run_id" in source.columns and "run_id_short" not in result.columns:
        result.insert(
            0,
            "run_id_short",
            pd.Series(
                [_run_detail_link(run_id) for run_id in source["run_id"].tolist()],
                index=result.index,
                dtype="string",
            ),
        )
    if "duration_seconds" in result.columns:
        result["duration"] = [format_duration(value) for value in result["duration_seconds"].tolist()]
        result = cast(pd.DataFrame, result.drop(columns=["duration_seconds"]))
    for column in ("started_at", "completed_at"):
        if column in result.columns:
            result[column] = [format_datetime(value) for value in result[column].tolist()]
    for column in ("error_class", "error_message"):
        if column in result.columns:
            result[column] = [truncate_text(value) for value in result[column].tolist()]
    ordered_columns = [
        "queue",
        "run_id_short",
        "status",
        "domain",
        "flow_name",
        "run_kind",
        "started_at",
        "completed_at",
        "duration",
        "units_total",
        "units_failed",
        "rows_written",
        "rows_rejected",
        "error_class",
        "error_message",
    ]
    return cast(pd.DataFrame, result.loc[:, [column for column in ordered_columns if column in result.columns]])


def render_run_table(source: pd.DataFrame, *, key: str) -> None:
    """Render a run table with links to run detail pages.

    Args:
        source: Raw run rows including ``run_id``.
        key: Unique Streamlit widget key for the table instance.
    """
    _render_compact_run_table(source, key=key)


def render_action_queue(source: pd.DataFrame, *, key: str) -> None:
    """Render the unified stale and attention triage table.

    Args:
        source: Run rows including a ``queue`` column from :func:`merge_action_queue`.
        key: Unique Streamlit widget key for the table instance.
    """
    _render_compact_run_table(source, key=key, caption="Stale runs are still in running state.")


def _render_compact_run_table(source: pd.DataFrame, *, key: str, caption: str | None = None) -> None:
    """Render a compact run dataframe with run-detail links."""
    table = compact_run_frame(source)
    st.dataframe(
        styled_table(table),
        width="stretch",
        height=table_height(table),
        hide_index=True,
        key=key,
        row_height=_TABLE_ROW_HEIGHT,
        placeholder="-",
        column_config=table_column_config(
            table,
            extra={
                "run_id_short": st.column_config.LinkColumn(
                    "Run ID",
                    width="small",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
            },
        ),
    )
    st.caption(caption or "Click a run ID to inspect units and evidence.")


def _run_detail_link(run_id: object) -> str:
    """Build a short run detail link label for a dataframe cell."""
    if not run_id:
        return "-"
    return labeled_href(run_detail_href(run_id), short_id(run_id))
