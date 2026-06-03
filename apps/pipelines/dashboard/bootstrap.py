"""Streamlit app setup helpers for the pipeline audit dashboard."""

from __future__ import annotations

import os

import streamlit as st


def configure_dashboard_runtime() -> None:
    """Configure process environment and Streamlit page metadata."""
    _prefer_dashboard_motherduck_token()
    st.set_page_config(
        page_title="Pipeline Audit",
        page_icon="",
        layout="wide",
        initial_sidebar_state="collapsed",
    )


def _prefer_dashboard_motherduck_token() -> None:
    """Prefer the read-only dashboard MotherDuck token over the worker token."""
    token = os.getenv("DASHBOARD_MOTHERDUCK_TOKEN", "").strip()
    if token:
        os.environ["MOTHERDUCK_TOKEN"] = token
