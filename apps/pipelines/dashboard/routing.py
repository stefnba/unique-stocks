"""Query-param routing helpers for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import streamlit as st

from dashboard.constants import OVERVIEW_PAGE, RUN_PAGE, UNIT_PAGE


def current_route() -> dict[str, str | None]:
    """Read the active dashboard route from Streamlit query params.

    Returns:
        Mapping with ``page``, ``run_id``, and ``unit_id`` keys.
    """
    page = query_param("page") or OVERVIEW_PAGE
    if page not in {OVERVIEW_PAGE, RUN_PAGE, UNIT_PAGE}:
        page = OVERVIEW_PAGE
    return {"page": page, "run_id": query_param("run_id"), "unit_id": query_param("unit_id")}


def set_route(page: str, *, run_id: str | None = None, unit_id: str | None = None) -> None:
    """Update Streamlit query params for the requested dashboard route.

    Args:
        page: Target page slug such as ``overview``, ``run``, or ``unit``.
        run_id: Optional durable run identifier for drill-down routes.
        unit_id: Optional durable unit identifier for unit drill-down routes.
    """
    st.query_params["page"] = page
    if run_id:
        st.query_params["run_id"] = run_id
    elif "run_id" in st.query_params:
        del st.query_params["run_id"]
    if unit_id:
        st.query_params["unit_id"] = unit_id
    elif "unit_id" in st.query_params:
        del st.query_params["unit_id"]


def query_param(name: str) -> str | None:
    """Return one query-param value from the active Streamlit session.

    Args:
        name: Query parameter name.

    Returns:
        Parameter value as a string, or ``None`` when absent.
    """
    value = st.query_params.get(name)
    if value is None:
        return None
    if isinstance(value, list):
        return str(value[0]) if value else None
    return str(value)


def run_detail_href(run_id: object) -> str:
    """Build a relative href for a run detail route.

    Args:
        run_id: Durable run identifier.

    Returns:
        Bookmarkable relative URL for the run page.
    """
    return f"?page={RUN_PAGE}&run_id={quote(str(run_id), safe='')}"


def unit_detail_href(*, run_id: str, unit_id: object) -> str:
    """Build a relative href for a unit detail route.

    Args:
        run_id: Parent run identifier.
        unit_id: Durable work-unit identifier.

    Returns:
        Bookmarkable relative URL for the unit page.
    """
    return f"?page={UNIT_PAGE}&run_id={quote(run_id, safe='')}&unit_id={quote(str(unit_id), safe='')}"


def overview_href() -> str:
    """Build a relative href for the overview route.

    Returns:
        Bookmarkable relative URL for the overview page.
    """
    return f"?page={OVERVIEW_PAGE}"


def render_sidebar_navigation(route: dict[str, str | None]) -> None:
    """Render sidebar page navigation for the active route.

    Args:
        route: Current route mapping from :func:`current_route`.
    """
    st.sidebar.subheader("Pages")
    if st.sidebar.button("Overview", width="stretch", disabled=route["page"] == OVERVIEW_PAGE):
        set_route(OVERVIEW_PAGE)
        st.rerun()
    if route["page"] == RUN_PAGE:
        st.sidebar.caption("Current page: Run Detail")
    elif route["page"] == UNIT_PAGE:
        st.sidebar.caption("Current page: Unit Detail")


def render_breadcrumb(*parts: tuple[str, str | None]) -> None:
    """Render clickable breadcrumb links from label/href pairs.

    Args:
        *parts: Sequence of ``(label, href)`` tuples. ``href=None`` renders plain text.
    """
    links: list[str] = []
    for label, href in parts:
        if href:
            links.append(f"[{label}]({href})")
        else:
            links.append(label)
    st.markdown(" › ".join(links))


def navigate_on_row_selection(
    *,
    state: object,
    values: list[Any],
    key: str,
    page: str,
    run_id: str | None = None,
) -> None:
    """Navigate when the user selects a different dataframe row.

    Args:
        state: Streamlit dataframe selection state object.
        values: Underlying identifier list aligned with dataframe row order.
        key: Unique widget key used to track the previous selection.
        page: Target page slug, typically ``run`` or ``unit``.
        run_id: Parent run identifier required when ``page`` is ``unit``.
    """
    selected_index = selected_row_index(state)
    if selected_index is None:
        return

    session_key = f"{key}_selection"
    previous_index = st.session_state.get(session_key)
    if previous_index == selected_index:
        return

    st.session_state[session_key] = selected_index
    selected_value = value_at(values, selected_index)
    if selected_value is None:
        return

    if page == UNIT_PAGE:
        if run_id is None:
            return
        set_route(UNIT_PAGE, run_id=run_id, unit_id=str(selected_value))
    elif page == RUN_PAGE:
        set_route(RUN_PAGE, run_id=str(selected_value))
    st.rerun()


def selected_row_index(state: object) -> int | None:
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


def value_at(values: list[Any], index: int | None) -> Any | None:
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
