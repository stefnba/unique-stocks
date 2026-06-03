"""Shared rendering helpers for dashboard pages."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import streamlit as st

from dashboard.constants import CACHE_TTL_SECONDS
from dashboard.formatting import format_datetime
from dashboard.routing import render_breadcrumb


def render_page_header(
    *,
    title: str,
    caption: str,
    breadcrumb: tuple[tuple[str, str | None], ...],
) -> None:
    """Render breadcrumb, title, and caption for a dashboard route."""
    render_breadcrumb(*breadcrumb)
    st.title(title)
    st.caption(caption)


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
