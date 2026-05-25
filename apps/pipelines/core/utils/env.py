"""Environment helpers for the pipelines app."""

from typing import Literal, overload


@overload
def by_env[T](*, dev: T, prod: T) -> T: ...
@overload
def by_env(*, default: str, add_env: Literal["suffix", "prefix"]) -> str: ...


def by_env[T](
    *,
    dev: T | None = None,
    prod: T | None = None,
    default: str | None = None,
    add_env: Literal["suffix", "prefix"] | None = None,
) -> T | str:
    """Return an environment-specific value.

    Two calling styles:
    - ``by_env(dev=..., prod=...)`` — explicit values per environment.
    - ``by_env(default="base", add_env="suffix")`` — append/prepend ``-dev``
      or ``-prod`` to *default* (e.g. ``"base-dev"`` / ``"base-prod"``).
    """
    from config.settings import SETTINGS  # local import avoids circular deps

    env_tag = "prod" if SETTINGS.is_production else "dev"

    if default is not None and add_env is not None:
        return f"{default}-{env_tag}" if add_env == "suffix" else f"{env_tag}-{default}"

    if dev is not None or prod is not None:
        return prod if SETTINGS.is_production else dev  # type: ignore[return-value]

    raise ValueError("Provide either (dev, prod) or (default, add_env).")
