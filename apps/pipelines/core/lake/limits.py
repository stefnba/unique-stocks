"""Lake writer Prefect runtime guard."""

import sys
from collections.abc import Generator
from contextlib import contextmanager

import structlog
from prefect.concurrency.sync import concurrency

from core.global_limits import LAKE_WRITER_LIMIT, prefect_global_limits_fail_closed

logger = structlog.get_logger(__name__)
_lake_writer_limit_missing = False


@contextmanager
def lake_writer_limit(operation: str | None = None) -> Generator[None]:
    """Serialize writes to the shared lake when Prefect limits are configured.

    The default is fail-open so local development and first-run bootstrap do not
    break if the Prefect server has not had limits created yet. Set
    PREFECT_GLOBAL_LIMITS_FAIL_CLOSED=true only after limits are managed.
    """
    global _lake_writer_limit_missing

    fail_closed = prefect_global_limits_fail_closed()
    if _lake_writer_limit_missing and not fail_closed:
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
        if fail_closed:
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
