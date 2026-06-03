"""Streamlit entrypoint for monitoring pipeline audit runs."""

from __future__ import annotations

import streamlit as st

from dashboard.bootstrap import configure_dashboard_runtime
from dashboard.navigation import dashboard_pages
from dashboard.style import render_dashboard_style


def main() -> None:
    """Render the Streamlit dashboard and route to the active page."""
    configure_dashboard_runtime()
    render_dashboard_style()
    page = st.navigation(dashboard_pages(), position="top")
    page.run()


if __name__ == "__main__":
    main()
