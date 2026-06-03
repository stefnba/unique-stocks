"""Unit detail page for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.formatting import (
    format_duration,
    format_int,
    format_unit_title,
    json_value_parsed,
    jsonable,
    short_id,
)
from dashboard.loaders import load_unit_page
from dashboard.routing import overview_href, render_breadcrumb, run_detail_href
from dashboard.views.components import (
    render_key_fields,
    render_landing_table,
    render_rejection_table,
    render_unit_status_callout,
)


def render_unit_page(run_id: str | None, unit_id: str | None) -> None:
    """Render one work unit's drill-down route.

    Args:
        run_id: Parent run identifier.
        unit_id: Durable work-unit identifier.
    """
    run_href = run_detail_href(run_id) if run_id else None
    render_breadcrumb(
        ("Pipeline Audit", overview_href()),
        ("Run", run_href),
        ("Unit", None),
    )

    if not run_id or not unit_id:
        st.warning("Choose a work unit from a run detail page to open its detail page.")
        return

    try:
        page = load_unit_page(run_id=run_id, unit_id=unit_id)
    except Exception as exc:
        st.error("Unit detail is not reachable.")
        st.caption(f"{type(exc).__name__}: {exc}")
        return

    run = page["run"]
    unit = page["unit"]
    if run is None:
        st.warning(f"Run {run_id} was not found in pipeline.runs.")
        return
    if unit is None:
        st.warning(f"Unit {unit_id} was not found in pipeline.run_units for this run.")
        return

    _render_unit_summary(run, unit)
    _render_unit_detail(page["detail"])


def _render_unit_summary(run: dict[str, Any], unit: dict[str, Any]) -> None:
    """Render the unit header, status callout, and parsed key fields.

    Args:
        run: Parent run row from ``pipeline.runs``.
        unit: Work-unit row from ``pipeline.run_units``.
    """
    st.title(format_unit_title(unit))
    st.caption(
        " · ".join(
            value
            for value in (
                str(unit.get("unit_type") or ""),
                str(unit.get("domain") or ""),
                str(unit.get("provider") or ""),
                f"parent {run.get('flow_name')}",
                f"unit {short_id(unit.get('unit_id'))}",
            )
            if value
        )
    )
    if unit.get("unit_id"):
        st.markdown(f"Unit ID `{unit['unit_id']}`")
    render_unit_status_callout(unit)

    status, duration, raw, valid, written, rejected = st.columns(6)
    status.metric("Status", str(unit.get("status") or "-"))
    duration.metric("Duration", format_duration(unit.get("duration_seconds")))
    raw.metric("Rows Raw", format_int(unit.get("rows_raw")))
    valid.metric("Rows Valid", format_int(unit.get("rows_valid")))
    written.metric("Rows Written", format_int(unit.get("rows_written")))
    rejected.metric("Rows Rejected", format_int(unit.get("rows_rejected")))

    reason = unit.get("reason")
    if reason:
        st.info(f"Reason: {reason}")

    render_key_fields(unit.get("unit_key_json"))

    with st.expander("Raw Unit Key"):
        st.json(json_value_parsed(unit.get("unit_key_json")))

    with st.expander("Unit Metadata"):
        st.json(
            jsonable(
                {
                    "unit_id": unit.get("unit_id"),
                    "run_id": unit.get("run_id"),
                    "unit_key_hash": unit.get("unit_key_hash"),
                    "source_uri": unit.get("source_uri"),
                    "started_at": unit.get("started_at"),
                    "completed_at": unit.get("completed_at"),
                    "error_class": unit.get("error_class"),
                    "error_message": unit.get("error_message"),
                }
            )
        )


def _render_unit_detail(detail: dict[str, list[dict[str, Any]]]) -> None:
    """Render unit-scoped landing and rejection evidence tabs.

    Args:
        detail: Unit-scoped evidence lists loaded for the selected unit.
    """
    st.subheader("Unit Evidence")
    landing, rejections = st.tabs(
        [
            f"Landing Objects ({len(detail['landing_objects'])})",
            f"Rejections ({len(detail['rejections'])})",
        ]
    )
    with landing:
        render_landing_table(detail["landing_objects"])
    with rejections:
        render_rejection_table(detail["rejections"])
