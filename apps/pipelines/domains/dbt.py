"""Platform dbt flow wiring for domain-owned transformation assets."""

from prefect import flow

from core.transforms.dbt import DbtCommand, DbtIndirectSelection, run_dbt_build
from domains.dbt_assets import record_dbt_asset_materializations


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


__all__ = ["dbt_build_flow"]
