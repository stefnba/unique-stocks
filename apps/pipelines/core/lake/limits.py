"""Prefect global limit policies, runtime guards, and setup.

This module is reusable Prefect infrastructure. It knows how to register global
lake-writer limits and provider API-credit limits from already-supplied HTTP
client classes, but it does not discover or import this app's concrete provider
packages. App provider composition belongs in ``providers.registry`` and
``control_plane.prefect_setup``.
"""

import os
import sys
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import structlog
from prefect.concurrency.sync import concurrency

logger = structlog.get_logger(__name__)
_lake_writer_limit_missing = False
if TYPE_CHECKING:
    pass


LAKE_WRITER_LIMIT = "unique-stocks.lake-writer"


@contextmanager
def lake_writer_limit(operation: str | None = None) -> Generator[None]:
    """Serialize writes to the shared lake when Prefect limits are configured.

    The default is fail-open so local development and first-run bootstrap do not
    break if the Prefect server has not had limits created yet. Set
    PREFECT_GLOBAL_LIMITS_STRICT=true only after limits are managed.
    """
    global _lake_writer_limit_missing

    strict_limits = _strict_limits()
    if _lake_writer_limit_missing and not strict_limits:
        yield
        return

    manager = concurrency(
        LAKE_WRITER_LIMIT,
        occupy=1,
        strict=True,
        raise_on_lease_renewal_failure=False,
    )
    try:
        manager.__enter__()
    except Exception as exc:
        if strict_limits:
            raise
        _lake_writer_limit_missing = True
        logger.warning(
            "prefect_lake_writer_limit_unavailable",
            operation=operation,
            limit_name=LAKE_WRITER_LIMIT,
            error=str(exc),
        )
        yield
        return

    try:
        yield
    except BaseException:
        exc_type, exc, traceback = sys.exc_info()
        manager.__exit__(exc_type, exc, traceback)
        raise
    else:
        manager.__exit__(None, None, None)


def _strict_limits() -> bool:
    configured = _env_bool("PREFECT_GLOBAL_LIMITS_STRICT")
    return configured is True


def _global_limit_message(action: str, name: str, limit: int) -> str:
    return f"{action} global limit {name}: limit={limit}"


def _env_bool(name: str) -> bool | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false, got {raw!r}")
