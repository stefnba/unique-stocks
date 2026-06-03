"""Shared rendering helpers for dashboard pages."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import streamlit as st

from dashboard.constants import CACHE_TTL_SECONDS
from dashboard.formatting import format_datetime


def render_page_header(
    *,
    title: str,
    caption: str,
    breadcrumb: tuple[tuple[str, str | None], ...],
) -> None:
    """Render breadcrumb, title, and caption for a dashboard route."""
    render_breadcrumb_bar(*breadcrumb)
    st.title(title)
    st.caption(caption)


def render_breadcrumb_bar(*parts: tuple[str, str | None]) -> None:
    """Render breadcrumbs and the shared refresh action on one row.

    Args:
        *parts: Sequence of ``(label, href)`` tuples. ``href=None`` renders plain text.
    """
    breadcrumb_column, action_column = st.columns([10, 0.35], vertical_alignment="center")
    breadcrumb_column.markdown(_breadcrumb_markdown(*parts))
    if action_column.button(
        "",
        icon=":material/refresh:",
        help="Refresh dashboard data",
        width="stretch",
        key="dashboard_refresh",
    ):
        st.cache_data.clear()
        st.rerun()


def render_cache_caption(since: datetime) -> None:
    """Render the window and cache TTL caption."""
    st.caption(f"Window starts {format_datetime(since)}. Cache refreshes every {CACHE_TTL_SECONDS} seconds.")


def render_lake_unavailable() -> None:
    """Render the standard missing ``pipeline.runs`` warning."""
    st.warning("pipeline.runs is not available in the configured lake.")


def render_load_error(exc: Exception, *, label: str) -> None:
    """Render a standard lake load failure message."""
    st.error(f"{label} is not reachable.")
    st.caption(f"{type(exc).__name__}: {exc}")


def load_or_show_error[T](
    loader: Callable[[], T],
    *,
    error_label: str,
) -> T | None:
    """Call a loader and render errors instead of raising to the page."""
    try:
        return loader()
    except Exception as exc:
        render_load_error(exc, label=error_label)
        return None


def lake_ready(snapshot: dict[str, Any]) -> bool:
    """Return whether the snapshot reports lake availability."""
    if snapshot.get("available"):
        return True
    render_lake_unavailable()
    return False


def _breadcrumb_markdown(*parts: tuple[str, str | None]) -> str:
    """Return clickable breadcrumb markdown from label/href pairs."""
    links: list[str] = []
    for label, href in parts:
        if href:
            links.append(f"[{label}]({href})")
        else:
            links.append(label)
    return " › ".join(links)
