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
    TABLE_PAGE_SIZE_OPTIONS,
    UNIT_ATTENTION_STATUSES,
)
from dashboard.formatting import (
    format_datetime,
    format_duration,
    format_int,
    format_json_compact,
    format_key_value,
    humanize_column,
    json_dict,
    short_id,
    truncate_text,
)
from dashboard.routing import (
    labeled_href,
    landing_object_detail_href,
    run_detail_href,
    run_unit_preview_href,
    unit_detail_href,
)

_TABLE_HEADER_HEIGHT = 38
_TABLE_ROW_HEIGHT = 34
_MIN_VISIBLE_TABLE_ROWS = 3
_MAX_VISIBLE_TABLE_ROWS = 14

_SMALL_TEXT_COLUMNS = frozenset(
    {
        "domain",
        "provider",
        "queue",
        "reason",
        "run_kind",
        "status",
        "unit_type",
    }
)
_MEDIUM_TEXT_COLUMNS = frozenset(
    {
        "completed_at",
        "content_hash",
        "dataset",
        "error_class",
        "flow_name",
        "recorded_at",
        "started_at",
    }
)
_LARGE_TEXT_COLUMNS = frozenset(
    {
        "error_message",
        "partition_json",
        "raw_sample_json",
        "source_uri",
        "unit_key",
    }
)


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
            config[column] = st.column_config.NumberColumn(humanize_column(column), format="%,d", width="small")
        elif column in FLOAT_TABLE_COLUMNS:
            config[column] = st.column_config.NumberColumn(humanize_column(column), format="%,.2f", width="small")
        elif is_status_column(column):
            config[column] = st.column_config.TextColumn(humanize_column(column), width="small")
        else:
            config[column] = text_column_config(column)
    return config


def text_column_config(column: object) -> Any:
    """Build a width-aware text column configuration.

    Args:
        column: Dataframe column label.

    Returns:
        Streamlit text column config sized for common audit fields.
    """
    column_name = str(column)
    if column_name in _SMALL_TEXT_COLUMNS:
        return st.column_config.TextColumn(humanize_column(column), width="small")
    if column_name in _MEDIUM_TEXT_COLUMNS:
        return st.column_config.TextColumn(humanize_column(column), width="medium")
    if column_name in _LARGE_TEXT_COLUMNS:
        return st.column_config.TextColumn(humanize_column(column), width="large")
    return st.column_config.TextColumn(humanize_column(column))


def table_height(table: pd.DataFrame) -> int:
    """Return a bounded dataframe height that avoids page-size controls.

    Args:
        table: Display dataframe.

    Returns:
        Pixel height for Streamlit's scrollable dataframe canvas.
    """
    visible_rows = min(max(len(table.index), _MIN_VISIBLE_TABLE_ROWS), _MAX_VISIBLE_TABLE_ROWS)
    return _TABLE_HEADER_HEIGHT + (_TABLE_ROW_HEIGHT * visible_rows)


def paginated_frame(source: pd.DataFrame, *, key: str, label: str) -> pd.DataFrame:
    """Render pagination controls and return the selected frame slice.

    Args:
        source: Filtered dataframe to paginate.
        key: Unique Streamlit key prefix for pagination controls.
        label: Human-readable row label such as ``runs``.

    Returns:
        Current page of ``source``.
    """
    if source.empty or len(source.index) <= min(TABLE_PAGE_SIZE_OPTIONS):
        return source

    page_size_column, page_column, range_column = st.columns([1, 1, 3])
    page_size = int(
        cast(
            int,
            page_size_column.selectbox(
                "Page size",
                options=list(TABLE_PAGE_SIZE_OPTIONS),
                index=1,
                key=f"{key}_page_size",
            ),
        )
    )
    page_count = max(1, (len(source.index) + page_size - 1) // page_size)
    page_key = f"{key}_page"
    current_page = page_state_value(st.session_state.get(page_key, 1))
    if current_page > page_count:
        st.session_state[page_key] = page_count
        current_page = page_count

    page = int(
        cast(
            int,
            page_column.selectbox(
                "Page",
                options=list(range(1, page_count + 1)),
                index=current_page - 1,
                format_func=lambda value: f"{value} of {page_count}",
                key=page_key,
            ),
        )
    )
    start = (page - 1) * page_size
    end = min(start + page_size, len(source.index))
    range_column.caption(
        f"Showing {format_int(start + 1)}-{format_int(end)} of {format_int(len(source.index))} {label}."
    )
    return cast(pd.DataFrame, source.iloc[start:end].copy())


def table_browser_frame(
    source: pd.DataFrame,
    *,
    key: str,
    label: str,
    filter_column: str,
    filter_label: str,
    search_columns: list[str],
    search_placeholder: str,
    filter_default: str | None = None,
) -> pd.DataFrame:
    """Render compact browser controls and return the filtered page.

    Args:
        source: Dataframe after page-level scope filters.
        key: Unique Streamlit key prefix for controls.
        label: Human-readable row label such as ``runs``.
        filter_column: Column used for the table's relevant dropdown filter.
        filter_label: Human-readable filter label.
        search_columns: Columns included in text search.
        search_placeholder: Placeholder for the search input.
        filter_default: Optional initial filter value from query params.

    Returns:
        Current page after dropdown filtering, search, and pagination.
    """
    if source.empty:
        return source

    filter_column_ui, search_column_ui, page_size_column, page_column, range_column = st.columns([1.3, 2.7, 1, 1, 2])

    filtered = source
    options = _filter_options(source, column=filter_column)
    if options:
        all_label = f"All {filter_label.lower()}"
        filter_options = [all_label, *options]
        filter_index = filter_options.index(filter_default) if filter_default in filter_options else 0
        selected_filter = str(
            filter_column_ui.selectbox(
                filter_label,
                options=filter_options,
                index=filter_index,
                key=f"{key}_filter",
            )
        )
        if selected_filter != all_label:
            filtered = filter_frame_by_values(filtered, column=filter_column, values=[selected_filter])
    else:
        filter_column_ui.caption(f"No {filter_label.lower()} values")

    search_query = str(
        search_column_ui.text_input(
            "Search",
            placeholder=search_placeholder,
            key=f"{key}_search",
        )
    ).strip()
    filtered = search_frame(filtered, query=search_query, columns=search_columns)
    if filtered.empty:
        st.info(f"No {label} match the table controls.")
        return filtered

    page_size = int(
        cast(
            int,
            page_size_column.selectbox(
                "Page size",
                options=list(TABLE_PAGE_SIZE_OPTIONS),
                index=1,
                key=f"{key}_page_size",
            ),
        )
    )
    page_count = max(1, (len(filtered.index) + page_size - 1) // page_size)
    page_key = f"{key}_page"
    current_page = page_state_value(st.session_state.get(page_key, 1))
    if current_page > page_count:
        st.session_state[page_key] = page_count
        current_page = page_count
    page = int(
        cast(
            int,
            page_column.selectbox(
                "Page",
                options=list(range(1, page_count + 1)),
                index=current_page - 1,
                format_func=lambda value: f"{value} of {page_count}",
                key=page_key,
            ),
        )
    )
    start = (page - 1) * page_size
    end = min(start + page_size, len(filtered.index))
    range_column.caption(
        f"Showing {format_int(start + 1)}-{format_int(end)} of {format_int(len(filtered.index))} {label}."
    )
    return cast(pd.DataFrame, filtered.iloc[start:end].copy())


def page_state_value(value: object) -> int:
    """Parse a Streamlit page widget value into a one-based page number.

    Args:
        value: Session-state value, which may be an int or an old formatted label
            such as ``1 of 5``.

    Returns:
        Positive one-based page number.
    """
    if isinstance(value, int):
        return max(1, value)
    text = str(value or "").strip()
    if not text:
        return 1
    first_token = text.split(maxsplit=1)[0]
    try:
        return max(1, int(first_token))
    except ValueError:
        return 1


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


def compact_landing_object_frame(source: pd.DataFrame) -> pd.DataFrame:
    """Build the compact landing-object table shown on overview and detail pages.

    Args:
        source: Raw landing-object rows including ``landing_id``.

    Returns:
        Display-ready dataframe with links to landing, run, and unit detail pages.
    """
    columns = [
        "landing_id_short",
        "run_id_short",
        "unit_id_short",
        "domain",
        "dataset",
        "provider",
        "flow_name",
        "source_uri",
        "rows_raw",
        "byte_count",
        "content_hash",
        "partition_json",
        "recorded_at",
    ]
    result = _copy_available_columns(source, columns)
    if "landing_id" in source.columns and "landing_id_short" not in result.columns:
        result.insert(
            0,
            "landing_id_short",
            pd.Series(
                [
                    labeled_href(landing_object_detail_href(landing_id), short_id(landing_id))
                    for landing_id in source["landing_id"].tolist()
                ],
                index=result.index,
                dtype="string",
            ),
        )
    if "run_id" in source.columns and "run_id_short" not in result.columns:
        insert_at = 1 if "landing_id_short" in result.columns else 0
        result.insert(
            insert_at,
            "run_id_short",
            pd.Series(
                [labeled_href(run_detail_href(run_id), short_id(run_id)) for run_id in source["run_id"].tolist()],
                index=result.index,
                dtype="string",
            ),
        )
    if {"run_id", "unit_id"}.issubset(source.columns) and "unit_id_short" not in result.columns:
        insert_at = 2 if {"landing_id_short", "run_id_short"}.issubset(result.columns) else len(result.columns)
        result.insert(
            insert_at,
            "unit_id_short",
            pd.Series(
                [_unit_detail_link(row) for row in source.to_dict(orient="records")],
                index=result.index,
                dtype="string",
            ),
        )
    if "partition_json" in result.columns:
        result["partition_json"] = [format_json_compact(value) for value in result["partition_json"].tolist()]
    if "recorded_at" in result.columns:
        result["recorded_at"] = [format_datetime(value) for value in result["recorded_at"].tolist()]
    ordered_columns = [
        "landing_id_short",
        "run_id_short",
        "unit_id_short",
        "domain",
        "dataset",
        "provider",
        "flow_name",
        "recorded_at",
        "rows_raw",
        "byte_count",
        "content_hash",
        "partition_json",
        "source_uri",
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


def render_landing_object_overview_table(source: pd.DataFrame, *, key: str) -> None:
    """Render a landing-object overview table with links to evidence detail pages.

    Args:
        source: Raw landing-object rows including ``landing_id``.
        key: Unique Streamlit widget key for the table instance.
    """
    table = compact_landing_object_frame(source)
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
                "landing_id_short": st.column_config.LinkColumn(
                    "Landing ID",
                    width="small",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
                "run_id_short": st.column_config.LinkColumn(
                    "Run ID",
                    width="small",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
                "unit_id_short": st.column_config.LinkColumn(
                    "Unit ID",
                    width="small",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
            },
        ),
    )
    st.caption("Click a landing ID for object detail, or use run and unit IDs for parent context.")


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
        height=table_height(table),
        hide_index=True,
        row_height=_TABLE_ROW_HEIGHT,
        placeholder="-",
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


def _filter_options(source: pd.DataFrame, *, column: str) -> list[str]:
    """Return non-empty dropdown options for a dataframe column.

    Args:
        source: Input dataframe.
        column: Column to inspect.

    Returns:
        Unique values in dataframe order, converted to strings.
    """
    if column not in source.columns:
        return []
    values = source[column].dropna().astype("string").drop_duplicates().tolist()
    return [str(value) for value in values if str(value).strip() and str(value) != "<NA>"]


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


def _run_detail_link(run_id: object) -> str:
    """Build a short run detail link label for a dataframe cell."""
    if not run_id:
        return "-"
    return labeled_href(run_detail_href(run_id), short_id(run_id))


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
