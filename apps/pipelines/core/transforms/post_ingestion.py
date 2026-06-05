"""Post-ingestion dbt build orchestration."""

from typing import Any, Literal

import structlog
from prefect.deployments.flow_runs import arun_deployment

from core.ingestion.run_tracking import RunStatus

log = structlog.get_logger(__name__)

type DbtBuildDeployment = Literal[
    "exchange-build",
    "instrument-build",
    "price-build",
    "fundamental-build",
]


async def run_dbt_build_after_ingestion(
    *,
    enabled: bool,
    build: DbtBuildDeployment,
    upstream_status: RunStatus,
    parent_run_id: str,
) -> dict[str, object]:
    """Run a dbt deployment only after a clean ingestion audit status.

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
    """Launch a dbt build deployment and require a completed Prefect state."""
    deployment_name = f"dbt-build/{build}"
    parameters: dict[str, Any] = {"parent_run_id": parent_run_id}
    run_tags = tags or ["dbt", build]
    if idempotency_key is None:
        flow_run = await arun_deployment(
            deployment_name,
            parameters=parameters,
            tags=run_tags,
            as_subflow=True,
        )
    else:
        flow_run = await arun_deployment(
            deployment_name,
            parameters=parameters,
            idempotency_key=idempotency_key,
            tags=run_tags,
            as_subflow=True,
        )
    state = flow_run.state
    state_name = _state_name(state)
    state_type = _state_type(state)
    result: dict[str, object] = {
        "enabled": True,
        "triggered": True,
        "build": build,
        "deployment": deployment_name,
        "flow_run_id": str(flow_run.id),
        "state_name": state_name,
        "state_type": state_type,
    }
    if state is None or not state.is_completed():
        log.error(
            "dbt.build_deployment_failed",
            build=build,
            parent_run_id=parent_run_id,
            flow_run_id=str(flow_run.id),
            state_name=state_name,
            state_type=state_type,
        )
        raise RuntimeError(
            f"Post-ingestion dbt deployment {deployment_name} finished in state "
            f"{state_name or state_type or 'unknown'}."
        )

    log.info(
        "dbt.build_deployment_done",
        build=build,
        parent_run_id=parent_run_id,
        flow_run_id=str(flow_run.id),
        state_name=state_name,
        state_type=state_type,
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


def _state_name(state: Any | None) -> str | None:
    """Return a Prefect state name without depending on concrete state internals."""
    value = getattr(state, "name", None)
    return value if isinstance(value, str) else None


def _state_type(state: Any | None) -> str | None:
    """Return a Prefect state type as a serializable string."""
    value = getattr(state, "type", None)
    enum_value = getattr(value, "value", value)
    return enum_value if isinstance(enum_value, str) else None


__all__ = ["DbtBuildDeployment", "run_dbt_build_after_ingestion", "run_dbt_build_deployment"]
