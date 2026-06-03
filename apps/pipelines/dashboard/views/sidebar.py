"""Sidebar filter controls for the overview page."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TypedDict, cast

import streamlit as st

from dashboard.formatting import format_window
from dashboard.queries import DEFAULT_DASHBOARD_DOMAINS, RUN_STATUSES


class SidebarFilters(TypedDict):
    """Typed values collected from dashboard controls.

    Attributes:
        since: Start of the selected overview window.
        stale_after: Timestamp before which ``running`` runs are considered stale.
        domains: Selected ingestion domains.
        statuses: Status filters applied only to the lookup explorer.
        recent_limit: Maximum number of lookup rows to fetch.
    """

    since: datetime
    stale_after: datetime
    domains: list[str]
    statuses: list[str]
    recent_limit: int


def render_sidebar() -> SidebarFilters:
    """Render overview sidebar controls and return normalized filter values.

    Returns:
        Parsed sidebar filters used by the overview snapshot loader.
    """
    now = datetime.now(UTC)
    window_hours = int(
        cast(
            int,
            st.sidebar.selectbox(
                "Window",
                options=[24, 72, 168, 336, 720],
                index=2,
                format_func=format_window,
            ),
        )
    )
    domains = [
        str(value)
        for value in st.sidebar.multiselect(
            "Domains",
            options=list(DEFAULT_DASHBOARD_DOMAINS),
            default=list(DEFAULT_DASHBOARD_DOMAINS),
        )
    ]
    statuses = [
        str(value)
        for value in st.sidebar.multiselect(
            "Lookup statuses",
            options=list(RUN_STATUSES),
            default=list(RUN_STATUSES),
            help="Applies to Run Lookup only. Triage always shows failed, partial, and stale runs.",
        )
    ]
    stale_hours = int(st.sidebar.number_input("Stale running hours", min_value=1, max_value=72, value=2, step=1))
    recent_limit = int(st.sidebar.slider("Lookup rows", min_value=25, max_value=500, value=150, step=25))

    return {
        "since": now - timedelta(hours=window_hours),
        "stale_after": now - timedelta(hours=stale_hours),
        "domains": domains,
        "statuses": statuses,
        "recent_limit": recent_limit,
    }
