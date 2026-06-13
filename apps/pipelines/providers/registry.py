"""Provider registry.

Subpackages import ``Provider`` from here; keep this module free of provider
subpackage imports to avoid circular imports.
"""

from enum import StrEnum


class Provider(StrEnum):
    """Known external data providers."""

    EODHD = "eodhd"
    ISO10383 = "iso10383"


__all__ = ["Provider"]
