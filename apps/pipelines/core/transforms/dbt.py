"""Prefect flow for dbt transformations with lake audit writes."""

from __future__ import annotations

import inspect
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import structlog
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from pydantic import BaseModel, ConfigDict

from config.settings import APP_ROOT, DbtTarget, get_settings
from core.clients.lake import get_lake_client, reset_lake_client
from core.ingestion import PipelineRunTracker, RunCounters, terminal_status
from core.ingestion.serialization import jsonable
from core.lake.database import ensure_lake_database
from core.prefect_controls import emit_dbt_failed_event, lake_writer_limit, materialize_dbt_assets

log = structlog.get_logger(__name__)

type DbtCommand = Literal["build", "run", "test", "compile"]
type DbtIndirectSelection = Literal["eager", "cautious", "buildable", "empty"]


class DbtCommandResult(BaseModel):
    """Result of one dbt CLI process."""

    model_config = ConfigDict(extra="forbid")

    command_args: list[str]
    return_code: int
    stdout: str
    stderr: str
    started_at: datetime
    completed_at: datetime
    elapsed_seconds: float
    artifact_path: str | None


@flow(
    name="dbt-build",
    description=(
        "Run dbt (build/run/test/compile) against the lake, then persist run_results.json "
        "to pipeline.dbt_invocations and pipeline.dbt_node_results for audit."
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
    """Run dbt as a transformation flow and persist its ``run_results.json`` artifact."""
    select = select or []
    exclude = exclude or []
    settings = get_settings()
    resolved_target = _resolve_dbt_target(target, settings.resolved_dbt_target(), settings.lake_backend())
    project_path = _resolve_app_path(project_dir)
    profiles_path = _resolve_app_path(profiles_dir)
    tracker = PipelineRunTracker()
    artifact: dict[str, Any] | None = None
    result: DbtCommandResult | None = None
    summary: dict[str, object] = {"command": command}

    with tracker.track_run(
        flow_name="dbt-build",
        domain="dbt",
        run_kind=command,
        parameters={
            "command": command,
            "select": select,
            "exclude": exclude,
            "indirect_selection": indirect_selection,
            "project_dir": str(project_path),
            "profiles_dir": str(profiles_path),
            "target": resolved_target,
        },
        parent_run_id=parent_run_id,
    ) as run:
        try:
            dbt_run_id = str(uuid.uuid4())
            target_path = project_path / "target" / "pipeline-runs" / dbt_run_id
            _release_local_lake_lock()
            result = run_dbt_command(
                command=command,
                select=select,
                exclude=exclude,
                indirect_selection=indirect_selection,
                project_dir=str(project_path),
                profiles_dir=str(profiles_path),
                target=resolved_target,
                target_path=str(target_path),
            )
            _refresh_tracker_lake(tracker)
            artifact = read_dbt_run_results(target_path=str(target_path))
            summary.update(
                {
                    "return_code": result.return_code,
                    "artifact_path": result.artifact_path,
                }
            )
            _record_dbt_invocation(
                dbt_run_id=dbt_run_id,
                run_id=run.run_id,
                result=result,
                artifact=artifact,
                project_dir=str(project_path),
            )
            node_count = _record_dbt_node_results(dbt_run_id=dbt_run_id, artifact=artifact)
            failed_nodes = _failed_node_count(artifact)
            summary["node_results"] = node_count
            await _create_dbt_summary_artifact(
                dbt_run_id=dbt_run_id,
                command=command,
                target=resolved_target,
                result=result,
                node_count=node_count,
                failed_nodes=failed_nodes,
                parent_run_id=parent_run_id,
            )
            if result.return_code != 0 or failed_nodes > 0:
                emit_dbt_failed_event(
                    dbt_run_id=dbt_run_id,
                    app_run_id=run.run_id,
                    command=command,
                    target=resolved_target,
                    return_code=result.return_code,
                    failed_nodes=failed_nodes,
                    artifact_path=result.artifact_path,
                )
            elif command in {"build", "run"}:
                materialize_dbt_assets(
                    select=select,
                    metadata={
                        "dbt_run_id": dbt_run_id,
                        "app_run_id": run.run_id,
                        "command": command,
                        "target": resolved_target,
                        "node_results": node_count,
                        "artifact_path": result.artifact_path,
                    },
                )

            if result.return_code != 0:
                message = _dbt_error_message(result)
                error = RuntimeError(message)
                run.fail(
                    error,
                    counters=RunCounters(units_total=node_count, units_failed=failed_nodes),
                    summary=summary,
                )
                raise error

            run.complete(
                status=terminal_status(failed=failed_nodes),
                counters=RunCounters(
                    units_total=node_count,
                    units_succeeded=node_count - failed_nodes,
                    units_failed=failed_nodes,
                ),
                summary=summary,
            )
            return summary
        except Exception as exc:
            if not run.is_terminal:
                node_count = _node_result_count(artifact)
                failed_nodes = _failed_node_count(artifact)
                run.fail(
                    exc,
                    counters=RunCounters(
                        units_total=node_count,
                        units_failed=failed_nodes if node_count is not None else None,
                    ),
                    summary=summary,
                )
            raise


@task(name="run-dbt-command")
def run_dbt_command(
    *,
    command: DbtCommand,
    select: list[str],
    exclude: list[str],
    indirect_selection: DbtIndirectSelection | None = "buildable",
    project_dir: str,
    profiles_dir: str,
    target: str | None,
    target_path: str,
) -> DbtCommandResult:
    """Run dbt in a subprocess and return captured process metadata."""
    settings = get_settings()
    resolved_target = _resolve_dbt_target(target, settings.resolved_dbt_target(), settings.lake_backend())
    if resolved_target == "prod":
        ensure_lake_database(settings)
    project_path = _resolve_app_path(project_dir)
    profiles_path = _resolve_app_path(profiles_dir)
    resolved_target_path = _resolve_app_path(target_path)
    resolved_target_path.mkdir(parents=True, exist_ok=True)
    args = _dbt_base_command()
    args.extend([command, "--project-dir", str(project_path), "--profiles-dir", str(profiles_path)])
    args.extend(["--target", resolved_target])
    args.extend(["--target-path", str(resolved_target_path)])
    if select:
        args.extend(["--select", *select])
    if exclude:
        args.extend(["--exclude", *exclude])
    if indirect_selection and command in {"build", "test"}:
        args.extend(["--indirect-selection", indirect_selection])

    started = _now()
    log.info(
        "dbt.command_start",
        command=command,
        project_dir=str(project_path),
        target=resolved_target,
        select=select,
        indirect_selection=indirect_selection,
    )
    with lake_writer_limit(f"dbt.{command}"):
        completed_process = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            cwd=APP_ROOT,
            env={**os.environ, **settings.dbt_env_overlay()},
        )
    completed = _now()
    elapsed = max(0.0, (completed - started).total_seconds())
    artifact_path = str(resolved_target_path / "run_results.json")
    log.info("dbt.command_done", command=command, return_code=completed_process.returncode, elapsed_seconds=elapsed)
    return DbtCommandResult(
        command_args=args,
        return_code=completed_process.returncode,
        stdout=completed_process.stdout,
        stderr=completed_process.stderr,
        started_at=started,
        completed_at=completed,
        elapsed_seconds=elapsed,
        artifact_path=artifact_path if Path(artifact_path).exists() else None,
    )


@task(name="read-dbt-run-results")
def read_dbt_run_results(*, target_path: str) -> dict[str, Any] | None:
    """Read dbt's ``run_results.json`` artifact when dbt produced one."""
    path = _resolve_app_path(target_path) / "run_results.json"
    if not path.exists():
        log.warning("dbt.run_results_missing", path=str(path))
        return None
    return json.loads(path.read_text())


def _record_dbt_invocation(
    *,
    dbt_run_id: str,
    run_id: str,
    result: DbtCommandResult,
    artifact: dict[str, Any] | None,
    project_dir: str,
) -> None:
    """Insert one row into ``pipeline.dbt_invocations``."""
    metadata = artifact.get("metadata", {}) if artifact else {}
    args = artifact.get("args", {}) if artifact else {}
    lake = get_lake_client()
    lake.insert_rows(
        "pipeline",
        "dbt_invocations",
        [
            {
                "dbt_run_id": dbt_run_id,
                "run_id": run_id,
                "dbt_invocation_id": _safe_uuid(metadata.get("invocation_id")),
                "command": _command_from_args(result.command_args),
                "command_args_json": result.command_args,
                "project_dir": project_dir,
                "profiles_dir": _arg_after(result.command_args, "--profiles-dir") or "",
                "target": _arg_after(result.command_args, "--target"),
                "status": "completed" if result.return_code == 0 else "failed",
                "return_code": result.return_code,
                "started_at": result.started_at,
                "completed_at": result.completed_at,
                "elapsed_seconds": result.elapsed_seconds,
                "artifact_path": result.artifact_path,
                "artifact_metadata_json": jsonable({"metadata": metadata, "args": args}),
                "error_message": _dbt_error_message(result) if result.return_code != 0 else None,
            }
        ],
    )


async def _create_dbt_summary_artifact(
    *,
    dbt_run_id: str,
    command: DbtCommand,
    target: str,
    result: DbtCommandResult,
    node_count: int,
    failed_nodes: int,
    parent_run_id: str | None,
) -> None:
    """Publish a compact dbt invocation summary to Prefect."""
    status = "completed" if result.return_code == 0 else "failed"
    artifact_path = result.artifact_path or "(not produced)"
    parent_line = f"- Parent run: `{parent_run_id}`\n" if parent_run_id else ""
    markdown = (
        f"# dbt {command} {status}\n\n"
        f"- dbt run: `{dbt_run_id}`\n"
        f"{parent_line}"
        f"- Target: `{target}`\n"
        f"- Return code: `{result.return_code}`\n"
        f"- Node results: `{node_count}`\n"
        f"- Failed nodes: `{failed_nodes}`\n"
        f"- Artifact: `{artifact_path}`\n"
        f"- Elapsed seconds: `{result.elapsed_seconds:.2f}`\n"
    )
    try:
        artifact_id = create_markdown_artifact(
            key=f"dbt-{dbt_run_id}",
            markdown=markdown,
            description=f"dbt {command} {status}",
        )
        if inspect.isawaitable(artifact_id):
            await artifact_id
    except Exception:
        log.warning("dbt.prefect_artifact_failed", dbt_run_id=dbt_run_id, exc_info=True)


def _record_dbt_node_results(*, dbt_run_id: str, artifact: dict[str, Any] | None) -> int:
    """Insert per-node rows from dbt's ``run_results.json`` artifact."""
    if not artifact:
        return 0
    rows = []
    for result in artifact.get("results", []):
        if not isinstance(result, dict):
            continue
        adapter_response = result.get("adapter_response")
        if not isinstance(adapter_response, dict):
            adapter_response = None
        unique_id = str(result.get("unique_id") or "")
        rows.append(
            {
                "dbt_run_id": dbt_run_id,
                "unique_id": unique_id,
                "resource_type": unique_id.split(".", 1)[0] if "." in unique_id else None,
                "status": str(result.get("status") or "unknown"),
                "execution_time": result.get("execution_time"),
                "failures": result.get("failures"),
                "message": result.get("message"),
                "adapter_response_json": jsonable(adapter_response) if adapter_response is not None else None,
                "rows_affected": _rows_affected(adapter_response),
                "relation_name": result.get("relation_name"),
                "compiled": result.get("compiled"),
            }
        )
    if rows:
        get_lake_client().insert_rows("pipeline", "dbt_node_results", rows)
    return len(rows)


def _dbt_base_command() -> list[str]:
    """Return a dbt executable command that works in uv and deployed environments."""
    if dbt_path := shutil.which("dbt"):
        return [dbt_path]
    return [sys.executable, "-m", "dbt.cli.main"]


def _resolve_app_path(value: str) -> Path:
    """Resolve an app-relative path against the pipelines app root."""
    path = Path(value)
    if path.is_absolute():
        return path
    return APP_ROOT / path


def _resolve_dbt_target(target: str | None, expected: DbtTarget, backend: str) -> DbtTarget:
    """Return the derived dbt target, rejecting explicit target drift."""
    if target is not None and target != expected:
        raise ValueError(f"dbt target {target!r} conflicts with lake backend {backend!r}; expected {expected!r}.")
    return expected


def _command_from_args(args: list[str]) -> str:
    """Return the dbt subcommand from a full command line."""
    for arg in args:
        if arg in {"build", "run", "test", "compile"}:
            return arg
    return "unknown"


def _arg_after(args: list[str], flag: str) -> str | None:
    try:
        index = args.index(flag)
    except ValueError:
        return None
    if index + 1 >= len(args):
        return None
    return args[index + 1]


def _rows_affected(adapter_response: dict[str, Any] | None) -> int | None:
    if not adapter_response:
        return None
    value = adapter_response.get("rows_affected")
    return int(value) if isinstance(value, int) else None


def _failed_node_count(artifact: dict[str, Any] | None) -> int:
    if not artifact:
        return 0
    failed_statuses = {"error", "fail", "runtime error"}
    return sum(
        1
        for result in artifact.get("results", [])
        if isinstance(result, dict) and str(result.get("status", "")).lower() in failed_statuses
    )


def _node_result_count(artifact: dict[str, Any] | None) -> int | None:
    """Return the number of dbt node result objects when an artifact is available."""
    if not artifact:
        return None
    return sum(1 for result in artifact.get("results", []) if isinstance(result, dict))


def _release_local_lake_lock() -> None:
    """Close cached lake handles so the dbt subprocess can lock the local DuckDB file."""
    if get_settings().lake_backend() != "local":
        return
    reset_lake_client()


def _refresh_tracker_lake(tracker: PipelineRunTracker) -> None:
    """Point run tracking at the process-wide lake client after a release."""
    tracker.lake = get_lake_client()


def _dbt_error_message(result: DbtCommandResult) -> str:
    """Return a compact dbt error message suitable for audit rows."""
    text = result.stderr.strip() or result.stdout.strip() or f"dbt exited with return code {result.return_code}"
    if "Could not set lock on file" in text:
        prefix = (
            "DuckDB file lock conflict on the local lake database. "
            "Close other writers (ingestion flows, DB viewers) and retry. "
        )
        text = prefix + text
    return text[-2000:]


def _safe_uuid(value: object) -> str | None:
    if value is None:
        return None
    try:
        return str(uuid.UUID(str(value)))
    except ValueError:
        return None


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


__all__ = ["dbt_build_flow", "read_dbt_run_results", "run_dbt_command"]
