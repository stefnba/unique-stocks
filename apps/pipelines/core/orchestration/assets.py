"""Generic Prefect asset materialization helpers."""

from collections.abc import Callable

import structlog
from prefect.context import get_run_context
from structlog.typing import FilteringBoundLogger

logger: FilteringBoundLogger = structlog.get_logger(__name__)


def record_prefect_materialization(
    *,
    materialization_name: str,
    materializer: Callable[..., object],
    metadata: dict[str, object],
) -> None:
    """Record one Prefect asset materialization without failing caller work."""
    try:
        get_run_context()
    except RuntimeError:
        logger.debug(
            "prefect_asset_materialization_skipped",
            materialization_name=materialization_name,
            reason="missing_run_context",
        )
        return

    try:
        materializer(**metadata)
    except Exception as exc:
        logger.warning(
            "prefect_asset_materialization_failed",
            materialization_name=materialization_name,
            error=str(exc),
        )
