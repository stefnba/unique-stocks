"""Shared filter types and control rendering for dashboard pages."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TypedDict, cast

import streamlit as st
from streamlit.delta_generator import DeltaGenerator

from dashboard.constants import UNIT_STATUSES
from dashboard.formatting import format_window
from dashboard.queries import DEFAULT_DASHBOARD_DOMAINS, RUN_STATUSES
from dashboard.routing import query_param

WINDOW_HOUR_OPTIONS = (24, 72, 168, 336, 720)
ALL_DOMAINS_LABEL = "All domains"
ALL_STATUSES_LABEL = "All statuses"


class ScopeFilters(TypedDict):
    """Window, stale threshold, and domain scope shared by overview and domains."""

    since: datetime
    stale_after: datetime
    domains: list[str]


class RunFilters(TypedDict):
    """Filters for the run browser page."""

    since: datetime
    domains: list[str]
    statuses: list[str]
    recent_limit: int
    search: str


class RunUnitFilters(TypedDict):
    """Filters for the run-unit browser page."""

    since: datetime
    domains: list[str]
    statuses: list[str]
    recent_limit: int
    run_id: str | None
    search: str


class LandingObjectFilters(TypedDict):
    """Filters for the landing-object browser page."""

    since: datetime
    domains: list[str]
    recent_limit: int
    run_id: str | None
    unit_id: str | None
    search: str


def domains_from_label(label: str) -> list[str]:
    """Convert a domain selectbox label into query domain filters."""
    if label == ALL_DOMAINS_LABEL:
        return []
    return [label]


def render_scope_controls(
    *,
    key_prefix: str,
    include_stale: bool,
    domain_default: str | None = None,
) -> ScopeFilters:
    """Render window, optional stale threshold, and domain scope controls."""
    now = datetime.now(UTC)
    window_hours, stale_hours, domains = _window_domain_row(
        key_prefix=key_prefix,
        include_stale=include_stale,
        domain_default=domain_default,
    )
    return {
        "since": now - timedelta(hours=window_hours),
        "stale_after": now - timedelta(hours=stale_hours),
        "domains": domains,
    }


def render_run_controls() -> RunFilters:
    """Render run browser filters and return normalized values."""
    now = datetime.now(UTC)
    domain_default = query_param("domain")
    status_default = query_param("status")

    with st.container(border=True):
        window_column, domain_column, status_column, limit_column = st.columns([1, 2, 2, 1])
        window_hours = _select_window(window_column, key="runs_window")
        focus_domain = _select_domain(domain_column, key="runs_domain", default=domain_default)
        status_scope = _select_status(
            status_column,
            key="runs_status",
            options=[ALL_STATUSES_LABEL, *list(RUN_STATUSES)],
            default=status_default,
        )
        recent_limit = int(
            limit_column.slider(
                "Rows",
                min_value=25,
                max_value=500,
                value=150,
                step=25,
                key="runs_rows",
            )
        )
        search = str(
            st.text_input(
                "Search",
                placeholder="Run id, flow, domain, provider, error, or message",
                key="runs_search",
            )
        ).strip()

    return {
        "since": now - timedelta(hours=window_hours),
        "domains": domains_from_label(focus_domain),
        "statuses": [] if status_scope == ALL_STATUSES_LABEL else [status_scope],
        "recent_limit": recent_limit,
        "search": search,
    }


def render_window_control(*, key_prefix: str) -> datetime:
    """Render a single window selector and return its lower timestamp bound."""
    now = datetime.now(UTC)
    with st.container(border=True):
        window_column, _ = st.columns([1, 3])
        window_hours = int(
            cast(
                int,
                window_column.selectbox(
                    "Window",
                    options=list(WINDOW_HOUR_OPTIONS),
                    index=2,
                    format_func=format_window,
                    key=f"{key_prefix}_window",
                ),
            )
        )
    return now - timedelta(hours=window_hours)


def render_run_unit_controls() -> RunUnitFilters:
    """Render run-unit browser filters and return normalized values."""
    now = datetime.now(UTC)
    domain_default = query_param("domain")
    status_default = query_param("status")
    with st.container(border=True):
        window_column, domain_column, status_column, limit_column = st.columns([1, 2, 2, 1])
        window_hours = _select_window(window_column, key="run_units_window")
        focus_domain = _select_domain(domain_column, key="run_units_domain", default=domain_default)
        status_scope = _select_status(
            status_column,
            key="run_units_status",
            options=[ALL_STATUSES_LABEL, *list(UNIT_STATUSES)],
            default=status_default,
        )
        recent_limit = int(
            limit_column.slider(
                "Rows",
                min_value=25,
                max_value=500,
                value=150,
                step=25,
                key="run_units_rows",
            )
        )
        run_column, search_column = st.columns([2, 3])
        run_id = str(
            run_column.text_input(
                "Run ID",
                value=query_param("run_id") or "",
                placeholder="Optional parent run id",
                key="run_units_run_id",
            )
        ).strip()
        search = str(
            search_column.text_input(
                "Search",
                placeholder="Unit id, key, hash, reason, source URI, or error",
                key="run_units_search",
            )
        ).strip()

    return {
        "since": now - timedelta(hours=window_hours),
        "domains": domains_from_label(focus_domain),
        "statuses": [] if status_scope == ALL_STATUSES_LABEL else [status_scope],
        "recent_limit": recent_limit,
        "run_id": run_id or None,
        "search": search,
    }


def render_landing_object_controls() -> LandingObjectFilters:
    """Render landing-object browser filters and return normalized values."""
    now = datetime.now(UTC)
    with st.container(border=True):
        window_column, domain_column, limit_column = st.columns([1, 2, 1])
        window_hours = _select_window(window_column, key="landing_objects_window")
        focus_domain = _select_domain(domain_column, key="landing_objects_domain", default=query_param("domain"))
        recent_limit = int(
            limit_column.slider(
                "Rows",
                min_value=25,
                max_value=500,
                value=150,
                step=25,
                key="landing_objects_rows",
            )
        )
        run_column, unit_column, search_column = st.columns([2, 2, 3])
        run_id = str(
            run_column.text_input(
                "Run ID",
                value=query_param("run_id") or "",
                placeholder="Optional run id",
                key="landing_objects_run_id",
            )
        ).strip()
        unit_id = str(
            unit_column.text_input(
                "Unit ID",
                value=query_param("unit_id") or "",
                placeholder="Optional unit id",
                key="landing_objects_unit_id",
            )
        ).strip()
        search = str(
            search_column.text_input(
                "Search",
                placeholder="Landing id, dataset, provider, source URI, hash, or partition",
                key="landing_objects_search",
            )
        ).strip()

    return {
        "since": now - timedelta(hours=window_hours),
        "domains": domains_from_label(focus_domain),
        "recent_limit": recent_limit,
        "run_id": run_id or None,
        "unit_id": unit_id or None,
        "search": search,
    }


def _window_domain_row(
    *,
    key_prefix: str,
    include_stale: bool,
    domain_default: str | None,
) -> tuple[int, int, list[str]]:
    """Render window, optional stale, and domain controls in one bordered row."""
    with st.container(border=True):
        if include_stale:
            window_column, stale_column, domain_column = st.columns([1, 1, 2])
        else:
            window_column, domain_column = st.columns([1, 2])
            stale_column = None

        window_hours = int(
            cast(
                int,
                window_column.selectbox(
                    "Window",
                    options=list(WINDOW_HOUR_OPTIONS),
                    index=2,
                    format_func=format_window,
                    key=f"{key_prefix}_window",
                ),
            )
        )
        stale_hours = 2
        if stale_column is not None:
            stale_hours = int(
                stale_column.number_input(
                    "Stale hours",
                    min_value=1,
                    max_value=72,
                    value=2,
                    step=1,
                    key=f"{key_prefix}_stale_hours",
                )
            )
        domain_options = [ALL_DOMAINS_LABEL, *list(DEFAULT_DASHBOARD_DOMAINS)]
        domain_index = _option_index(domain_options, domain_default, offset=0)
        focus_domain = str(
            domain_column.selectbox(
                "Domain",
                options=domain_options,
                index=domain_index,
                key=f"{key_prefix}_domain",
            )
        )

    return window_hours, stale_hours, domains_from_label(focus_domain)


def _select_window(container: DeltaGenerator, *, key: str) -> int:
    """Render a standard window selectbox in a provided container."""
    return int(
        cast(
            int,
            container.selectbox(
                "Window",
                options=list(WINDOW_HOUR_OPTIONS),
                index=2,
                format_func=format_window,
                key=key,
            ),
        )
    )


def _select_domain(
    container: DeltaGenerator,
    *,
    key: str,
    default: str | None,
) -> str:
    """Render a standard domain selectbox in a provided container."""
    domain_options = [ALL_DOMAINS_LABEL, *list(DEFAULT_DASHBOARD_DOMAINS)]
    domain_index = _option_index(domain_options, default, offset=0)
    return str(
        container.selectbox(
            "Domain",
            options=domain_options,
            index=domain_index,
            key=key,
        )
    )


def _select_status(
    container: DeltaGenerator,
    *,
    key: str,
    options: list[str],
    default: str | None,
) -> str:
    """Render a standard status selectbox in a provided container."""
    status_index = _option_index(options, default, offset=0)
    return str(
        container.selectbox(
            "Status",
            options=options,
            index=status_index,
            key=key,
        )
    )


def _option_index(options: list[str], value: str | None, *, offset: int) -> int:
    """Return a safe selectbox index for an optional default value."""
    if not value or value not in options:
        return offset
    return options.index(value)
