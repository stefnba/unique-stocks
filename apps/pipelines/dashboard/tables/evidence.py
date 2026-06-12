"""Generic compact evidence table rendering helpers."""

from __future__ import annotations

from typing import cast

import pandas as pd
import streamlit as st

from dashboard.formatting import format_datetime, format_json_compact, short_id
from dashboard.tables.base import _TABLE_ROW_HEIGHT, styled_table, table_column_config, table_height


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
