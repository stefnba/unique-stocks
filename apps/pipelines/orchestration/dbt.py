"""Application dbt Prefect flow wiring.

This module is app orchestration, not reusable core. It binds the generic dbt
command/audit helper in ``core.transforms.dbt`` to this app's concrete
domain-owned asset materialization registry.

Keep generic dbt subprocess execution and audit persistence in ``core``. Keep
domain-specific asset grouping and deployment entrypoints here in
``orchestration``.
"""

from prefect import flow

from config.settings import APP_ROOT, get_settings
from core.transforms.dbt import DbtCommand, DbtIndirectSelection, DbtRuntimeContext, run_dbt_build
from orchestration.dbt_assets import record_dbt_asset_materializations


@flow(
    name="dbt-build",
    description=(
        "Run dbt (build/run/test/compile) against the lake, persist run_results.json "
        "for audit, and record domain-owned Silver/Gold asset materializations."
    ),
)
async def dbt_build_flow(
    command: DbtCommand = "build",
    select: list[str] | None = None,
    exclude: list[str] | None = None,
    indirect_selection: DbtIndirectSelection | None = "buildable",
    project_dir: str = "dbt",
    profiles_dir: str = "dbt",
    target: str | None = None,
    parent_run_id: str | None = None,
) -> dict[str, object]:
    """Run the platform dbt build flow with domain-owned asset materializations."""
    return await run_dbt_build(
        runtime=dbt_runtime_context(),
        command=command,
        select=select,
        exclude=exclude,
        indirect_selection=indirect_selection,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        parent_run_id=parent_run_id,
        asset_materializer=record_dbt_asset_materializations,
    )


def dbt_runtime_context() -> DbtRuntimeContext:
    """Build the generic dbt runtime context from app settings."""
    settings = get_settings()
    return DbtRuntimeContext(
        app_root=str(APP_ROOT),
        expected_target=settings.resolved_dbt_target(),
        lake_backend=settings.lake_backend(),
        env_overlay=settings.dbt_env_overlay(),
        local_lake_path=settings.resolved_local_lake_path(),
        motherduck_database_name=settings.motherduck_database_name,
        motherduck_token=settings.motherduck_token.get_secret_value(),
    )


__all__ = ["dbt_build_flow", "dbt_runtime_context"]
