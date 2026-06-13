"""Stable provider identity keys for the pipelines app.

Executable provider wiring belongs in ``registry.provider_registry``.
"""

from enum import StrEnum


class Provider(StrEnum):
    """Known external data providers for this app."""

    EODHD = "eodhd"
    ISO10383 = "iso10383"


__all__ = ["Provider"]
