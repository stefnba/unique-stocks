"""Tests for generic registry helpers."""

import pytest

from core.registry import ordered_registry_values


def test_ordered_registry_values_returns_values_in_key_order() -> None:
    """Registry values should follow the supplied key order."""
    registry = {"b": 2, "a": 1}

    assert ordered_registry_values(registry, ("a", "b"), label="Demo") == (1, 2)


def test_ordered_registry_values_fails_on_missing_keys() -> None:
    """Incomplete registries should fail with a clear label."""
    with pytest.raises(RuntimeError, match="Demo registry is missing keys: c"):
        ordered_registry_values({"a": 1}, ("a", "c"), label="Demo")
