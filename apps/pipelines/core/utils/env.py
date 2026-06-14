"""Generic environment selection helpers.

These helpers are dependency-free utilities. Callers provide the active
environment state explicitly so ``core`` does not import app settings or own
app-specific environment decisions.
"""

from typing import Literal, overload


@overload
def by_env[T](*, is_production: bool, dev: T, prod: T) -> T: ...
@overload
def by_env(*, is_production: bool, default: str, add_env: Literal["suffix", "prefix"]) -> str: ...


def by_env[T](
    *,
    is_production: bool,
    dev: T | None = None,
    prod: T | None = None,
    default: str | None = None,
    add_env: Literal["suffix", "prefix"] | None = None,
) -> T | str:
    """Return an environment-specific value from explicit environment state."""
    env_tag = "prod" if is_production else "dev"

    if default is not None and add_env is not None:
        return f"{default}-{env_tag}" if add_env == "suffix" else f"{env_tag}-{default}"

    if dev is not None or prod is not None:
        if dev is None or prod is None:
            raise ValueError("Provide both dev and prod values.")
        return prod if is_production else dev

    raise ValueError("Provide either (dev, prod) or (default, add_env).")


__all__ = ["by_env"]
