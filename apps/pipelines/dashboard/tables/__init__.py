"""Public table helper facade for the dashboard."""

from __future__ import annotations

from dashboard.tables.base import (
    all_filter_label,
    filter_frame_by_values,
    frame,
    is_status_column,
    page_state_value,
    paginated_frame,
    search_frame,
    status_cell_style,
    styled_table,
    table_browser_frame,
    table_column_config,
    table_height,
    text_column_config,
)
from dashboard.tables.evidence import render_compact_dataframe
from dashboard.tables.landing_objects import compact_landing_object_frame, render_landing_object_overview_table
from dashboard.tables.run_units import (
    compact_run_unit_overview_frame,
    compact_unit_frame,
    default_unit_statuses,
    render_run_unit_overview_table,
    render_unit_table,
    unit_key_column_name,
    unit_key_columns_frame,
)
from dashboard.tables.runs import compact_run_frame, merge_action_queue, render_action_queue, render_run_table

__all__ = [
    "all_filter_label",
    "compact_landing_object_frame",
    "compact_run_frame",
    "compact_run_unit_overview_frame",
    "compact_unit_frame",
    "default_unit_statuses",
    "filter_frame_by_values",
    "frame",
    "is_status_column",
    "merge_action_queue",
    "page_state_value",
    "paginated_frame",
    "render_action_queue",
    "render_compact_dataframe",
    "render_landing_object_overview_table",
    "render_run_table",
    "render_run_unit_overview_table",
    "render_unit_table",
    "search_frame",
    "status_cell_style",
    "styled_table",
    "table_browser_frame",
    "table_column_config",
    "table_height",
    "text_column_config",
    "unit_key_column_name",
    "unit_key_columns_frame",
]
