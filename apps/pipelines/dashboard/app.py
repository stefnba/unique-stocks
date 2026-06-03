"""Streamlit entrypoint for monitoring pipeline audit runs."""

from __future__ import annotations

import os

import streamlit as st

from dashboard.constants import RUN_PAGE, UNIT_PAGE
from dashboard.routing import current_route, render_sidebar_navigation
from dashboard.views.overview import render_overview_page
from dashboard.views.run_page import render_run_page
from dashboard.views.unit_page import render_unit_page


def main() -> None:
    """Render the Streamlit dashboard and route to the active page."""
    _prefer_dashboard_motherduck_token()
    st.set_page_config(
        page_title="Pipeline Runs",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    route = current_route()
    render_sidebar_navigation(route)

    if st.sidebar.button("Refresh", width="stretch"):
        st.cache_data.clear()

    if route["page"] == RUN_PAGE:
        render_run_page(route.get("run_id"))
        return

    if route["page"] == UNIT_PAGE:
        render_unit_page(route.get("run_id"), route.get("unit_id"))
        return

    render_overview_page()


def _prefer_dashboard_motherduck_token() -> None:
    """Prefer the read-only dashboard MotherDuck token over the worker token."""
    token = os.getenv("DASHBOARD_MOTHERDUCK_TOKEN", "").strip()
    if token:
        os.environ["MOTHERDUCK_TOKEN"] = token


if __name__ == "__main__":
    main()
