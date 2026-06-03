"""Shared redaction helpers for logs and errors."""

from __future__ import annotations

import re
from typing import Final

REDACTED_LOG_VALUE: Final[str] = "[redacted]"
REDACTED_QUERY_VALUE: Final[str] = REDACTED_LOG_VALUE
SENSITIVE_KEY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?i)(api[_-]?key|api[_-]?token|apikey|access[_-]?token|auth[_-]?token|authorization|"
    r"credential|motherduck[_-]?token|secret|password|token)"
)
SENSITIVE_QUERY_PARAM_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?i)([?&](?:api[_-]?key|api[_-]?token|apikey|access[_-]?token|auth[_-]?token|token|secret|password)=)"
    r"([^&#\s\"']*)"
)


def redact_sensitive_query_params(value: str) -> str:
    """Redact sensitive query parameter values from loggable text."""
    return SENSITIVE_QUERY_PARAM_PATTERN.sub(rf"\1{REDACTED_QUERY_VALUE}", value)


__all__ = [
    "REDACTED_LOG_VALUE",
    "REDACTED_QUERY_VALUE",
    "SENSITIVE_KEY_PATTERN",
    "redact_sensitive_query_params",
]
