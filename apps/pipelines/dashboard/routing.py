"""Route helpers for the pipeline audit dashboard."""

from __future__ import annotations

from urllib.parse import quote

import streamlit as st

from dashboard.constants import (
    DOMAIN_DETAIL_PAGE,
    DOMAINS_PAGE,
    LANDING_OBJECT_DETAIL_PAGE,
    LANDING_OBJECTS_PAGE,
    RUN_DETAIL_PAGE,
    RUN_UNIT_DETAIL_PAGE,
    RUN_UNITS_PAGE,
    RUNS_PAGE,
)


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
    return _page_href(RUN_DETAIL_PAGE, run_id=str(run_id))


def unit_detail_href(*, run_id: str, unit_id: object) -> str:
    """Build a relative href for a unit detail route.

    Args:
        run_id: Parent run identifier.
        unit_id: Durable work-unit identifier.

    Returns:
        Bookmarkable relative URL for the unit page.
    """
    return _page_href(RUN_UNIT_DETAIL_PAGE, run_id=run_id, unit_id=str(unit_id))


def run_unit_preview_href(*, run_id: str, unit_id: object) -> str:
    """Build a relative href for inline unit preview on the run page.

    Args:
        run_id: Parent run identifier.
        unit_id: Durable work-unit identifier to preview.

    Returns:
        Bookmarkable relative URL that opens the run page with preview active.
    """
    return _page_href(RUN_DETAIL_PAGE, run_id=run_id, preview_unit_id=str(unit_id))


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
    """Build a relative href for the overview route."""
    return "./"


def runs_href(*, domain: str | None = None, status: str | None = None) -> str:
    """Build a relative href for the run overview route with optional preset filters."""
    return _page_href(RUNS_PAGE, domain=domain, status=status)


def domains_href(*, domain: str | None = None) -> str:
    """Build a relative href for the domains route with optional domain focus."""
    return _page_href(DOMAINS_PAGE, domain=domain)


def domain_detail_href(domain: object) -> str:
    """Build a relative href for one domain detail route."""
    return _page_href(DOMAIN_DETAIL_PAGE, domain=str(domain))


def run_units_href(
    *,
    domain: str | None = None,
    status: str | None = None,
    run_id: str | None = None,
) -> str:
    """Build a relative href for the run-unit overview route."""
    return _page_href(RUN_UNITS_PAGE, domain=domain, status=status, run_id=run_id)


def landing_objects_href(
    *,
    domain: str | None = None,
    run_id: str | None = None,
    unit_id: str | None = None,
) -> str:
    """Build a relative href for the landing-object overview route."""
    return _page_href(LANDING_OBJECTS_PAGE, domain=domain, run_id=run_id, unit_id=unit_id)


def landing_object_detail_href(landing_id: object) -> str:
    """Build a relative href for one landing-object detail route."""
    return _page_href(LANDING_OBJECT_DETAIL_PAGE, landing_id=str(landing_id))


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


def _page_href(path: str, **params: str | None) -> str:
    """Build a Streamlit page-relative URL that preserves app base paths."""
    query = "&".join(
        f"{quote(key, safe='')}={quote(value, safe='')}" for key, value in params.items() if value is not None
    )
    if not query:
        return path
    return f"{path}?{query}"
