"""Landing-object table shaping and rendering helpers."""

from __future__ import annotations

from typing import cast

import pandas as pd
import streamlit as st

from dashboard.constants import LINK_COLUMN_LABEL_PATTERN
from dashboard.formatting import format_datetime, format_json_compact, short_id
from dashboard.routing import labeled_href, landing_object_detail_href, run_detail_href
from dashboard.tables.base import (
    _TABLE_ROW_HEIGHT,
    _copy_available_columns,
    styled_table,
    table_column_config,
    table_height,
)
from dashboard.tables.run_units import _unit_detail_link


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
