"""Table rendering helpers for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any, cast

import pandas as pd
import streamlit as st
from pandas.io.formats.style import Styler

from dashboard.constants import (
    FLOAT_TABLE_COLUMNS,
    INTEGER_TABLE_COLUMNS,
    LINK_COLUMN_LABEL_PATTERN,
    UNIT_ATTENTION_STATUSES,
)
from dashboard.formatting import (
    format_datetime,
    format_duration,
    format_json_compact,
    format_key_value,
    humanize_column,
    json_dict,
    short_id,
    truncate_text,
)
from dashboard.routing import labeled_href, run_detail_href, run_unit_preview_href


def frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert audit rows into a pandas dataframe.

    Args:
        rows: Lake query result rows.

    Returns:
        Dataframe indexed in query order.
    """
    return pd.DataFrame(rows)


def styled_table(table: pd.DataFrame) -> pd.DataFrame | Styler:
    """Apply status-aware styling when a table contains status columns.

    Args:
        table: Display dataframe.

    Returns:
        Original dataframe or a styled variant when status columns are present.
    """
    status_columns = [column for column in table.columns if is_status_column(column)]
    if not status_columns:
        return table
    return table.style.map(status_cell_style, subset=status_columns)


def table_column_config(table: pd.DataFrame, *, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build Streamlit column configuration for common audit table fields.

    Args:
        table: Display dataframe whose columns should receive defaults.
        extra: Optional preconfigured column settings that override defaults.

    Returns:
        Streamlit ``column_config`` mapping for ``st.dataframe``.
    """
    config = dict(extra or {})
    for column in table.columns:
        if column in config:
            continue
        if column in INTEGER_TABLE_COLUMNS:
            config[column] = st.column_config.NumberColumn(humanize_column(column), format="%,d")
        elif column in FLOAT_TABLE_COLUMNS:
            config[column] = st.column_config.NumberColumn(humanize_column(column), format="%,.2f")
        elif is_status_column(column):
            config[column] = st.column_config.TextColumn(humanize_column(column), width="small")
    return config


def is_status_column(column: object) -> bool:
    """Return whether a column should receive status styling.

    Args:
        column: Column name or label.

    Returns:
        ``True`` when the column represents a status field.
    """
    column_name = str(column)
    return column_name == "status" or column_name.endswith("_status")


def status_cell_style(value: object) -> str:
    """Return CSS for a status cell based on audit status semantics.

    Args:
        value: Cell value from a status column.

    Returns:
        Inline CSS string for pandas Styler, or an empty string for neutral cells.
    """
    status = str(value or "").strip().lower()
    if not status or status == "-":
        return ""
    if any(token in status for token in ("failed", "failure", "error", "rejected")):
        return "background-color: #fee2e2; color: #991b1b; font-weight: 700"
    if any(token in status for token in ("partial", "unsupported", "warn")):
        return "background-color: #ffedd5; color: #9a3412; font-weight: 700"
    if any(token in status for token in ("running", "queued", "pending")):
        return "background-color: #dbeafe; color: #1e40af; font-weight: 700"
    if any(token in status for token in ("completed", "success", "pass", "healthy")):
        return "background-color: #dcfce7; color: #166534; font-weight: 700"
    if "skipped" in status:
        return "background-color: #f3f4f6; color: #374151; font-weight: 700"
    return "background-color: #f3f4f6; color: #374151; font-weight: 600"


def compact_run_frame(source: pd.DataFrame) -> pd.DataFrame:
    """Build the compact run table shown on overview and triage pages.

    Args:
        source: Raw run rows including ``run_id`` and timing fields.

    Returns:
        Display-ready dataframe with formatted timestamps and truncated errors.
    """
    columns = [
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
                [labeled_href(run_detail_href(run_id), short_id(run_id)) for run_id in source["run_id"].tolist()],
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
    if "error_message" in result.columns:
        result["error_message"] = [truncate_text(value) for value in result["error_message"].tolist()]
    ordered_columns = [
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


def render_run_table(source: pd.DataFrame, *, key: str) -> None:
    """Render a run table with links to run detail pages.

    Args:
        source: Raw run rows including ``run_id``.
        key: Unique Streamlit widget key for the table instance.
    """
    table = compact_run_frame(source)
    st.dataframe(
        styled_table(table),
        width="stretch",
        hide_index=True,
        key=key,
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
    st.caption("Click a run ID to inspect units and evidence.")


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
        hide_index=True,
        key=key,
        on_select="rerun",
        selection_mode="single-row",
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


def render_compact_dataframe(source: pd.DataFrame, *, columns: list[str]) -> None:
    """Render a compact evidence table with formatted JSON and id columns.

    Args:
        source: Raw evidence rows.
        columns: Preferred column order. Missing columns are skipped.
    """
    available_columns = [column for column in columns if column in source.columns]
    table = cast(pd.DataFrame, source.loc[:, available_columns].copy())
    for column in ("recorded_at", "started_at", "completed_at"):
        if column in table.columns:
            table[column] = [format_datetime(value) for value in table[column].tolist()]
    for column in ("partition_json", "entity_key_json", "raw_sample_json"):
        if column in table.columns:
            table[column] = [format_json_compact(value) for value in table[column].tolist()]
    for column in ("run_id", "unit_id", "landing_id", "rejection_id"):
        if column in table.columns:
            table[column] = [short_id(value) for value in table[column].tolist()]
    st.dataframe(
        styled_table(table),
        width="stretch",
        hide_index=True,
        column_config=table_column_config(table),
    )


def filter_frame_by_values(source: pd.DataFrame, *, column: str, values: list[str]) -> pd.DataFrame:
    """Filter a dataframe to rows whose ``column`` value is in ``values``.

    Args:
        source: Input dataframe.
        column: Column name to filter on.
        values: Allowed string values. An empty list returns no rows.

    Returns:
        Filtered dataframe copy.
    """
    if column not in source.columns:
        return source
    if not values:
        return cast(pd.DataFrame, source.iloc[0:0].copy())
    mask = source[column].astype("string").isin(values)
    return cast(pd.DataFrame, source.loc[mask].copy())


def search_frame(source: pd.DataFrame, *, query: str, columns: list[str]) -> pd.DataFrame:
    """Filter a dataframe to rows matching a case-insensitive substring query.

    Args:
        source: Input dataframe.
        query: Free-text search string. Empty queries return the original frame.
        columns: Candidate columns to search.

    Returns:
        Filtered dataframe copy.
    """
    if not query:
        return source
    available_columns = [column for column in columns if column in source.columns]
    if not available_columns:
        return source
    mask = pd.Series(False, index=source.index)
    for column in available_columns:
        mask = mask | source[column].astype("string").str.contains(query, case=False, na=False, regex=False)
    return cast(pd.DataFrame, source.loc[mask].copy())


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


def _copy_available_columns(source: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Copy only the requested columns that exist in the source frame.

    Args:
        source: Input dataframe.
        columns: Desired column names.

    Returns:
        Subset dataframe copy.
    """
    available_columns = [column for column in columns if column in source.columns]
    return cast(pd.DataFrame, source.loc[:, available_columns].copy())


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
