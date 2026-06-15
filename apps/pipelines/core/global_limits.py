"""Canonical Prefect global limit names and shared runtime settings."""

from __future__ import annotations

import os
from typing import Final

LAKE_WRITER_LIMIT: Final = "unique-stocks.lake-writer"
HTTP_PROVIDER_LIMIT_PREFIX: Final = "unique-stocks.http.provider"
PREFECT_GLOBAL_LIMITS_FAIL_CLOSED_ENV_VAR: Final = "PREFECT_GLOBAL_LIMITS_FAIL_CLOSED"


def provider_rate_limit_name(provider: str) -> str:
    """Return the canonical Prefect global rate-limit name for a provider."""
    provider_key = provider.strip().lower().replace("_", "-")
    return f"{HTTP_PROVIDER_LIMIT_PREFIX}.{provider_key}"


def prefect_global_limits_fail_closed() -> bool:
    """Return whether missing Prefect global limits should fail closed."""
    configured = _env_bool(PREFECT_GLOBAL_LIMITS_FAIL_CLOSED_ENV_VAR)
    return configured is True


def _env_bool(name: str) -> bool | None:
    """Read an optional boolean environment variable."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false, got {raw!r}")
