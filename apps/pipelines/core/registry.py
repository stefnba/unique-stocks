"""Generic helpers for resolving app-owned registries."""

from collections.abc import Iterable, Mapping


def ordered_registry_values[K, V](
    registry: Mapping[K, V],
    keys: Iterable[K],
    *,
    label: str,
) -> tuple[V, ...]:
    """Return registry values in key order after validating completeness."""
    ordered_keys = tuple(keys)
    missing = tuple(key for key in ordered_keys if key not in registry)
    if missing:
        missing_keys = ", ".join(str(key) for key in missing)
        msg = f"{label} registry is missing keys: {missing_keys}"
        raise RuntimeError(msg)
    return tuple(registry[key] for key in ordered_keys)


__all__ = ["ordered_registry_values"]
