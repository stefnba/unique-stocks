"""Generic HTTP client registry contracts."""

from collections.abc import Mapping

from core.http.base import HttpClientBase

type HttpClientRegistry[K] = Mapping[K, type[HttpClientBase]]
"""HTTP client classes keyed by a caller-owned identity type."""

__all__ = ["HttpClientRegistry"]
