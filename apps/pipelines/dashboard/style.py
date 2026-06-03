"""Shared dashboard styling."""

from __future__ import annotations

import streamlit as st


def render_dashboard_style() -> None:
    """Apply compact operations-dashboard styling to Streamlit primitives."""
    st.markdown(
        """
        <style>
        :root {
            --audit-bg: #f7f8fa;
            --audit-border: #e3e7ee;
            --audit-muted: #667085;
        }
        .stApp {
            background: var(--audit-bg);
        }
        [data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid var(--audit-border);
            border-radius: 8px;
            padding: 0.75rem 0.85rem;
        }
        [data-testid="stMetricLabel"] p {
            color: var(--audit-muted);
            font-size: 0.76rem;
        }
        [data-testid="stMetricValue"] {
            font-size: 1.35rem;
            font-weight: 700;
        }
        .stDataFrame {
            border: 1px solid var(--audit-border);
            border-radius: 8px;
            overflow: hidden;
            background: #ffffff;
        }
        .stButton > button, .stLinkButton > a {
            border-radius: 6px;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
