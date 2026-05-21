"""Provider registry.

Subpackages (e.g. providers/eodhd/) import from here.
Do NOT import from subpackages in this file — that would create a circular dependency.
"""

from enum import StrEnum


class Provider(StrEnum):
    EODHD = "eodhd"


__all__ = ["Provider"]
