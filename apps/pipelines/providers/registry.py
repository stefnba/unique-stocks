"""Provider registry.

Subpackages (e.g. providers/eodhd/) import from here.
Do NOT import from subpackages in this file — that would create a circular dependency.
"""

from enum import StrEnum


class Provider(StrEnum):
    """Known external data providers."""

    EODHD = "eodhd"
    ISO10383 = "iso10383"


__all__ = ["Provider"]
