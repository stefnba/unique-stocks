"""Application post-ingestion dbt build orchestration.

This module owns the app-specific policy for running named dbt builds after
ingestion flows complete. It belongs in ``orchestration`` because build names
such as ``price-build`` and ``fundamental-build`` are deployment wiring, not
reusable core behavior.

The generic dbt subprocess and audit mechanics remain in ``core.transforms``.
Domain flows call this module when they need to promote successful Bronze writes
into Silver/Gold through a configured build selector.
"""

import structlog
from prefect import tags as prefect_tags

from config.settings import get_settings
from core.ingestion.run_tracking import RunStatus
from core.lake import reset_lake_client
from orchestration.dbt import dbt_build_flow
from orchestration.domain_dbt import DBT_BUILD_SPECS, DbtBuildDeployment

log = structlog.get_logger(__name__)


async def run_dbt_build_after_ingestion(
    *,
    enabled: bool,
    build: DbtBuildDeployment,
    upstream_status: RunStatus,
    parent_run_id: str,
) -> dict[str, object]:
    """Run a dbt build only after a clean ingestion audit status.

    This deliberately gates on the app's durable ``pipeline.runs.status`` value,
    not Prefect's flow-run state. Domain flows can finish normally while recording
    a ``partial`` audit status for per-unit failures; those runs must not promote
    Bronze changes to Silver/Gold automatically. If dbt fails after a completed
    ingestion audit, this helper raises so the parent Prefect flow also alerts.
    """
    if not enabled:
        return _skip_result(build=build, reason="disabled", upstream_status=upstream_status)
    if upstream_status != "completed":
        log.info(
            "dbt.post_ingestion_build_skipped",
            build=build,
            reason="upstream_not_completed",
            upstream_status=upstream_status,
            parent_run_id=parent_run_id,
        )
        return _skip_result(build=build, reason="upstream_not_completed", upstream_status=upstream_status)

    log.info("dbt.post_ingestion_build_start", build=build, parent_run_id=parent_run_id)
    return await run_dbt_build_deployment(
        build=build,
        parent_run_id=parent_run_id,
        idempotency_key=f"{parent_run_id}:{build}",
        tags=["post-ingestion-dbt", build],
    )


async def run_dbt_build_deployment(
    *,
    build: DbtBuildDeployment,
    parent_run_id: str | None,
    idempotency_key: str | None = None,
    tags: list[str] | None = None,
) -> dict[str, object]:
    """Run the configured dbt build and require a completed Prefect state.

    The public ``dbt-build/*`` deployments remain available for direct operator
    runs. When another flow needs a synchronous dbt build, call the dbt flow
    inline so a single-slot worker cannot deadlock waiting for a child
    deployment that needs the same worker.
    """
    deployment_name = f"dbt-build/{build}"
    run_tags = tags or ["dbt", build]
    _release_local_lake_lock_before_dbt(build=build, parent_run_id=parent_run_id)
    spec = DBT_BUILD_SPECS[build]
    with prefect_tags(*run_tags):
        dbt_summary = await dbt_build_flow(
            select=list(spec.select),
            parent_run_id=parent_run_id,
        )

    result: dict[str, object] = {
        "enabled": True,
        "triggered": True,
        "build": build,
        "deployment": deployment_name,
        "execution_mode": "inline",
        "idempotency_key": idempotency_key,
        "state_name": "Completed",
        "state_type": "COMPLETED",
        "dbt_summary": dbt_summary,
    }

    log.info(
        "dbt.build_inline_done",
        build=build,
        parent_run_id=parent_run_id,
        state_name=result["state_name"],
        state_type=result["state_type"],
    )
    return result


def _skip_result(*, build: DbtBuildDeployment, reason: str, upstream_status: RunStatus) -> dict[str, object]:
    """Return a stable non-trigger result for flow summaries."""
    return {
        "enabled": reason != "disabled",
        "triggered": False,
        "build": build,
        "reason": reason,
        "upstream_status": upstream_status,
    }


def _release_local_lake_lock_before_dbt(*, build: DbtBuildDeployment, parent_run_id: str | None) -> None:
    """Close this process's local DuckDB handle before a dbt build starts."""
    if get_settings().lake_backend() != "local":
        return
    reset_lake_client()
    log.info("dbt.local_lake_lock_released_before_build", build=build, parent_run_id=parent_run_id)


__all__ = ["run_dbt_build_after_ingestion", "run_dbt_build_deployment"]
