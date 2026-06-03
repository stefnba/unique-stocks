"""Query-param routing helpers for the pipeline audit dashboard."""

from __future__ import annotations

from urllib.parse import quote

import streamlit as st

from dashboard.constants import OVERVIEW_PAGE, RUN_PAGE, UNIT_PAGE


def current_route() -> dict[str, str | None]:
    """Read the active dashboard route from Streamlit query params.

    Returns:
        Mapping with ``page``, ``run_id``, ``unit_id``, and ``preview_unit_id`` keys.
    """
    page = query_param("page") or OVERVIEW_PAGE
    if page not in {OVERVIEW_PAGE, RUN_PAGE, UNIT_PAGE}:
        page = OVERVIEW_PAGE
    return {
        "page": page,
        "run_id": query_param("run_id"),
        "unit_id": query_param("unit_id"),
        "preview_unit_id": query_param("preview_unit_id"),
    }


def set_route(
    page: str,
    *,
    run_id: str | None = None,
    unit_id: str | None = None,
    preview_unit_id: str | None = None,
) -> None:
    """Update Streamlit query params for the requested dashboard route.

    Args:
        page: Target page slug such as ``overview``, ``run``, or ``unit``.
        run_id: Optional durable run identifier for drill-down routes.
        unit_id: Optional durable unit identifier for unit drill-down routes.
        preview_unit_id: Optional unit identifier for inline preview on the run page.
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
    if preview_unit_id:
        st.query_params["preview_unit_id"] = preview_unit_id
    elif "preview_unit_id" in st.query_params:
        del st.query_params["preview_unit_id"]


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


def run_unit_preview_href(*, run_id: str, unit_id: object) -> str:
    """Build a relative href for inline unit preview on the run page.

    Args:
        run_id: Parent run identifier.
        unit_id: Durable work-unit identifier to preview.

    Returns:
        Bookmarkable relative URL that opens the run page with preview active.
    """
    return f"?page={RUN_PAGE}&run_id={quote(run_id, safe='')}&preview_unit_id={quote(str(unit_id), safe='')}"


def labeled_href(href: str, label: str) -> str:
    """Append a URL fragment used as per-row LinkColumn display text.

    Streamlit extracts the fragment with a ``display_text`` regex while the browser
    ignores it when navigating.

    Args:
        href: Target relative URL.
        label: Human-readable link label, typically a shortened id.

    Returns:
        URL with ``#label`` appended for dataframe link columns.
    """
    return f"{href}#{label}"


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
