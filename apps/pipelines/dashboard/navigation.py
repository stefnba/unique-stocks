"""Native Streamlit page registration for the dashboard."""

from __future__ import annotations

from typing import Any

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
from dashboard.routing import query_param
from dashboard.views.domains import render_domain_detail_page, render_domains_page
from dashboard.views.landing_objects import render_landing_object_detail_page, render_landing_objects_page
from dashboard.views.overview import render_overview_page
from dashboard.views.run_page import render_run_page
from dashboard.views.run_units import render_run_units_page
from dashboard.views.runs import render_runs_page
from dashboard.views.unit_page import render_unit_page


def dashboard_pages() -> list[Any]:
    """Return Streamlit page definitions in primary navigation order."""
    return [
        st.Page(
            render_overview_page,
            title="Overview",
            icon=":material/dashboard:",
            default=True,
        ),
        st.Page(
            render_domains_page,
            title="Domains",
            icon=":material/account_tree:",
            url_path=DOMAINS_PAGE,
        ),
        st.Page(
            _render_domain_detail_route,
            title="Domain Detail",
            icon=":material/account_tree:",
            url_path=DOMAIN_DETAIL_PAGE,
            visibility="hidden",
        ),
        st.Page(
            render_runs_page,
            title="Runs",
            icon=":material/history:",
            url_path=RUNS_PAGE,
        ),
        st.Page(
            _render_run_detail_route,
            title="Run Detail",
            icon=":material/manage_search:",
            url_path=RUN_DETAIL_PAGE,
            visibility="hidden",
        ),
        st.Page(
            render_run_units_page,
            title="Run Units",
            icon=":material/view_list:",
            url_path=RUN_UNITS_PAGE,
        ),
        st.Page(
            _render_run_unit_detail_route,
            title="Run Unit Detail",
            icon=":material/list_alt:",
            url_path=RUN_UNIT_DETAIL_PAGE,
            visibility="hidden",
        ),
        st.Page(
            render_landing_objects_page,
            title="Landing Objects",
            icon=":material/cloud:",
            url_path=LANDING_OBJECTS_PAGE,
        ),
        st.Page(
            _render_landing_object_detail_route,
            title="Landing Object Detail",
            icon=":material/cloud_done:",
            url_path=LANDING_OBJECT_DETAIL_PAGE,
            visibility="hidden",
        ),
    ]


def _render_domain_detail_route() -> None:
    """Render the hidden domain detail route from query params."""
    render_domain_detail_page(query_param("domain"))


def _render_run_detail_route() -> None:
    """Render the hidden run detail route from query params."""
    render_run_page(query_param("run_id"), preview_unit_id=query_param("preview_unit_id"))


def _render_run_unit_detail_route() -> None:
    """Render the hidden run-unit detail route from query params."""
    render_unit_page(query_param("run_id"), query_param("unit_id"))


def _render_landing_object_detail_route() -> None:
    """Render the hidden landing-object detail route from query params."""
    render_landing_object_detail_page(query_param("landing_id"))
