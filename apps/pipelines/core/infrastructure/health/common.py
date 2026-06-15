"""Shared helpers for container and operational health checks."""

from __future__ import annotations

import urllib.request
from urllib.error import URLError


def prefect_api_is_healthy(api_url: str, *, timeout_seconds: float = 5.0) -> bool:
    """Return whether the configured Prefect API health endpoint responds."""
    health_url = api_url.rstrip("/") + "/health"
    try:
        with urllib.request.urlopen(health_url, timeout=timeout_seconds) as response:
            return 200 <= int(response.status) < 300
    except OSError, URLError:
        return False
