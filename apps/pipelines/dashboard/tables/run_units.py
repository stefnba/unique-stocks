"""Run-unit table shaping and rendering helpers."""

from __future__ import annotations

from typing import Any, cast

import pandas as pd
import streamlit as st

from dashboard.constants import LINK_COLUMN_LABEL_PATTERN, UNIT_ATTENTION_STATUSES
from dashboard.formatting import (
    format_datetime,
    format_duration,
    format_json_compact,
    format_key_value,
    json_dict,
    short_id,
    truncate_text,
)
from dashboard.routing import labeled_href, run_unit_preview_href, unit_detail_href
from dashboard.tables.base import (
    _TABLE_ROW_HEIGHT,
    _copy_available_columns,
    styled_table,
    table_column_config,
    table_height,
)
from dashboard.tables.runs import _run_detail_link


def compact_unit_frame(source: pd.DataFrame, *, run_id: str) -> pd.DataFrame:
    """Build the compact work-unit table shown on run detail pages.

    Args:
        source: Raw unit rows including ``unit_id`` and ``unit_key_json``.
        run_id: Parent run identifier used for detail and preview links.

    Returns:
        Display-ready dataframe with parsed unit-key columns when available.
    """
    columns = [
        "unit_id_short",
        "status",
        "unit_type",
        "unit_key_json",
        "reason",
        "error_class",
        "error_message",
        "rows_raw",
        "rows_valid",
        "rows_written",
        "rows_rejected",
        "duration_seconds",
        "source_uri",
    ]
    result = _copy_available_columns(source, columns)
    if "unit_id" in source.columns and "unit_id_short" not in result.columns:
        result.insert(
            0,
            "unit_id_short",
            pd.Series(
                [
                    labeled_href(
                        run_unit_preview_href(run_id=run_id, unit_id=unit_id),
                        short_id(unit_id),
                    )
                    for unit_id in source["unit_id"].tolist()
                ],
                index=result.index,
                dtype="string",
            ),
        )
    unit_key_frame, unit_key_columns = unit_key_columns_frame(source)
    if not unit_key_frame.empty:
        result = cast(pd.DataFrame, pd.concat([result, unit_key_frame], axis=1))
    if "unit_key_json" in result.columns:
        if not unit_key_columns:
            result["unit_key"] = [format_json_compact(value) for value in result["unit_key_json"].tolist()]
        result = cast(pd.DataFrame, result.drop(columns=["unit_key_json"]))
    if "duration_seconds" in result.columns:
        result["duration"] = [format_duration(value) for value in result["duration_seconds"].tolist()]
        result = cast(pd.DataFrame, result.drop(columns=["duration_seconds"]))
    if "error_message" in result.columns:
        result["error_message"] = [truncate_text(value) for value in result["error_message"].tolist()]
    ordered_columns = [
        "unit_id_short",
        "status",
        "unit_type",
        *unit_key_columns,
        "unit_key",
        "reason",
        "error_class",
        "error_message",
        "rows_raw",
        "rows_valid",
        "rows_written",
        "rows_rejected",
        "duration",
        "source_uri",
    ]
    return cast(pd.DataFrame, result.loc[:, [column for column in ordered_columns if column in result.columns]])


def compact_run_unit_overview_frame(source: pd.DataFrame) -> pd.DataFrame:
    """Build the compact work-unit table shown on the run-unit overview page.

    Args:
        source: Raw unit rows including ``unit_id`` and ``run_id``.

    Returns:
        Display-ready dataframe with links to unit and run detail pages.
    """
    columns = [
        "unit_id_short",
        "run_id_short",
        "status",
        "domain",
        "flow_name",
        "run_kind",
        "provider",
        "unit_type",
        "unit_key_json",
        "reason",
        "error_class",
        "error_message",
        "rows_raw",
        "rows_valid",
        "rows_written",
        "rows_rejected",
        "duration_seconds",
        "started_at",
        "completed_at",
        "source_uri",
    ]
    result = _copy_available_columns(source, columns)
    if "unit_id" in source.columns and "unit_id_short" not in result.columns:
        result.insert(
            0,
            "unit_id_short",
            pd.Series(
                [_unit_detail_link(row) for row in source.to_dict(orient="records")],
                index=result.index,
                dtype="string",
            ),
        )
    if "run_id" in source.columns and "run_id_short" not in result.columns:
        insert_at = 1 if "unit_id_short" in result.columns else 0
        result.insert(
            insert_at,
            "run_id_short",
            pd.Series(
                [_run_detail_link(run_id) for run_id in source["run_id"].tolist()],
                index=result.index,
                dtype="string",
            ),
        )
    unit_key_frame, unit_key_columns = unit_key_columns_frame(source)
    if not unit_key_frame.empty:
        result = cast(pd.DataFrame, pd.concat([result, unit_key_frame], axis=1))
    if "unit_key_json" in result.columns:
        if not unit_key_columns:
            result["unit_key"] = [format_json_compact(value) for value in result["unit_key_json"].tolist()]
        result = cast(pd.DataFrame, result.drop(columns=["unit_key_json"]))
    if "duration_seconds" in result.columns:
        result["duration"] = [format_duration(value) for value in result["duration_seconds"].tolist()]
        result = cast(pd.DataFrame, result.drop(columns=["duration_seconds"]))
    for column in ("started_at", "completed_at"):
        if column in result.columns:
            result[column] = [format_datetime(value) for value in result[column].tolist()]
    if "error_message" in result.columns:
        result["error_message"] = [truncate_text(value) for value in result["error_message"].tolist()]
    ordered_columns = [
        "unit_id_short",
        "run_id_short",
        "status",
        "domain",
        "flow_name",
        "run_kind",
        "provider",
        "unit_type",
        *unit_key_columns,
        "unit_key",
        "reason",
        "error_class",
        "error_message",
        "rows_raw",
        "rows_valid",
        "rows_written",
        "rows_rejected",
        "duration",
        "started_at",
        "completed_at",
        "source_uri",
    ]
    return cast(pd.DataFrame, result.loc[:, [column for column in ordered_columns if column in result.columns]])


def render_run_unit_overview_table(source: pd.DataFrame, *, key: str) -> None:
    """Render a run-unit overview table with links to unit and run detail pages.

    Args:
        source: Raw unit rows including ``unit_id`` and ``run_id``.
        key: Unique Streamlit widget key for the table instance.
    """
    table = compact_run_unit_overview_frame(source)
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
                "unit_id_short": st.column_config.LinkColumn(
                    "Unit ID",
                    width="small",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
                "run_id_short": st.column_config.LinkColumn(
                    "Run ID",
                    width="small",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
            },
        ),
    )
    st.caption("Click a unit ID for full evidence, or a run ID for the parent run investigation.")


def render_unit_table(source: pd.DataFrame, *, run_id: str, key: str) -> str | None:
    """Render a selectable unit table and return the selected unit id.

    Args:
        source: Filtered unit rows including ``unit_id``.
        run_id: Parent run identifier used for detail and preview links.
        key: Unique Streamlit widget key for the table instance.

    Returns:
        Selected unit id as a string, or ``None`` when no row is selected.
    """
    table = compact_unit_frame(source, run_id=run_id)
    state = st.dataframe(
        styled_table(table),
        width="stretch",
        height=table_height(table),
        hide_index=True,
        key=key,
        on_select="rerun",
        selection_mode="single-row",
        row_height=_TABLE_ROW_HEIGHT,
        placeholder="-",
        column_config=table_column_config(
            table,
            extra={
                "unit_id_short": st.column_config.LinkColumn(
                    "Unit ID",
                    width="small",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
            },
        ),
    )
    unit_ids = source["unit_id"].tolist() if "unit_id" in source.columns else []
    selected_index = _selected_index_from_state(state)
    selected_unit_id = _value_at(unit_ids, selected_index)
    st.caption("Click a unit ID or select a row to inspect evidence below.")
    return str(selected_unit_id) if selected_unit_id is not None else None


def default_unit_statuses(source: pd.DataFrame) -> list[str]:
    """Return the default unit-status filter for investigation tables.

    Args:
        source: Unit dataframe containing a ``status`` column.

    Returns:
        Attention statuses present in the frame, or all statuses when none match.
    """
    status_options = [str(value) for value in source["status"].dropna().unique().tolist()]
    return [status for status in status_options if status in UNIT_ATTENTION_STATUSES] or status_options


def unit_key_columns_frame(source: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Expand ``unit_key_json`` values into one column per key field.

    Args:
        source: Unit dataframe containing ``unit_key_json``.

    Returns:
        Tuple of parsed key dataframe and the ordered list of generated column names.
    """
    if "unit_key_json" not in source.columns:
        return pd.DataFrame(index=source.index), []

    unit_keys = [json_dict(value) for value in source["unit_key_json"].tolist()]
    key_names: list[str] = []
    for unit_key in unit_keys:
        for key in unit_key:
            column_name = unit_key_column_name(key, existing_columns=source.columns)
            if column_name not in key_names:
                key_names.append(column_name)

    if not key_names:
        return pd.DataFrame(index=source.index), []

    rows: list[dict[str, str]] = []
    for unit_key in unit_keys:
        row: dict[str, str] = {}
        for key, value in unit_key.items():
            row[unit_key_column_name(key, existing_columns=source.columns)] = format_key_value(value)
        rows.append(row)

    key_frame = pd.DataFrame(rows, index=source.index)
    return key_frame, key_names


def unit_key_column_name(key: object, *, existing_columns: pd.Index) -> str:
    """Build a safe dataframe column name for one unit-key field.

    Args:
        key: Raw JSON key from ``unit_key_json``.
        existing_columns: Existing dataframe columns used to avoid name collisions.

    Returns:
        Column name, prefixed with ``key_`` when it would collide.
    """
    column_name = str(key)
    if column_name in existing_columns:
        return f"key_{column_name}"
    return column_name


def _unit_detail_link(row: dict[str, Any]) -> str:
    """Build a short unit detail link label for a dataframe cell."""
    run_id = row.get("run_id")
    unit_id = row.get("unit_id")
    if not run_id or not unit_id:
        return "-"
    return labeled_href(unit_detail_href(run_id=str(run_id), unit_id=unit_id), short_id(unit_id))


def _selected_index_from_state(state: object) -> int | None:
    """Extract the first selected row index from a Streamlit dataframe state.

    Args:
        state: Streamlit dataframe widget return value.

    Returns:
        Zero-based selected row index, or ``None`` when nothing is selected.
    """
    selection = getattr(state, "selection", None)
    if selection is None and isinstance(state, dict):
        selection = state.get("selection")
    rows = getattr(selection, "rows", None)
    if rows is None and isinstance(selection, dict):
        rows = selection.get("rows")
    if not rows:
        return None
    return int(rows[0])


def _value_at(values: list[Any], index: int | None) -> Any | None:
    """Return the list value at an index when in bounds.

    Args:
        values: Source list aligned with dataframe rows.
        index: Selected row index.

    Returns:
        Value at ``index``, or ``None`` when the index is invalid.
    """
    if index is None or index < 0 or index >= len(values):
        return None
    return values[index]
