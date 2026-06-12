"""Base dataframe helpers for dashboard tables."""

from __future__ import annotations

from typing import Any, cast

import pandas as pd
import streamlit as st
from pandas.io.formats.style import Styler

from dashboard.constants import FLOAT_TABLE_COLUMNS, INTEGER_TABLE_COLUMNS, TABLE_PAGE_SIZE_OPTIONS
from dashboard.formatting import format_int, humanize_column

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
    page_key = f"{key}_page_{page_count}"
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
        all_label = all_filter_label(filter_label)
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
    page_key = f"{key}_page_{page_count}"
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


def all_filter_label(filter_label: str) -> str:
    """Return the all-values label for a table dropdown filter."""
    label = filter_label.lower()
    if label == "status":
        return "All statuses"
    if label.endswith("s"):
        return f"All {label}"
    return f"All {label}s"


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
