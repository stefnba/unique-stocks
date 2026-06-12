"""Import helpers for lake migration CLI references."""

from __future__ import annotations

import importlib
from collections.abc import Sequence

from core.lake.schema.table import TableModel


def load_table_specs(ref: str) -> tuple[type[TableModel], ...]:
    """Load table specs from a ``module:attribute`` reference."""
    module_name, separator, attr_name = ref.partition(":")
    if not module_name or separator != ":" or not attr_name:
        raise ValueError(f"Invalid table spec reference {ref!r}; expected module:attribute")

    value = getattr(importlib.import_module(module_name), attr_name)
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise TypeError(f"{ref} must reference a sequence of TableModel classes")

    tables = tuple(value)
    invalid = tuple(table for table in tables if not isinstance(table, type) or not issubclass(table, TableModel))
    if invalid:
        raise TypeError(f"{ref} contains non-TableModel entries: {invalid!r}")
    return tables


__all__ = ["load_table_specs"]
