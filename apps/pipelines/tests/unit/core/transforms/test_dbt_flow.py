"""Tests for dbt flow runtime configuration."""

from __future__ import annotations

import subprocess
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path

import pytest
from pydantic import SecretStr
from pytest import MonkeyPatch

from config.settings import APP_ROOT, Settings
from core.transforms import dbt


def test_record_dbt_asset_materializations_calls_hook() -> None:
    """Core dbt orchestration should call the injected asset materializer hook."""
    calls: list[dict[str, object]] = []

    def record_assets(*, select: Sequence[str], metadata: dict[str, object]) -> None:
        calls.append({"select": list(select), "metadata": metadata})

    dbt._record_dbt_asset_materializations(
        asset_materializer=record_assets,
        select=["path:models/marts/price"],
        metadata={"dbt_run_id": "run-1"},
    )

    assert calls == [{"select": ["path:models/marts/price"], "metadata": {"dbt_run_id": "run-1"}}]


def test_run_dbt_command_uses_app_root_paths_and_env_overlay(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dbt subprocesses should run from app root with derived dbt env."""
    settings = Settings(local_lake_path="unique_stocks.duckdb", motherduck_token=SecretStr(""))
    captured: dict[str, object] = {}

    def fake_run(
        args: Sequence[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
        cwd: Path,
        env: Mapping[str, str],
    ) -> subprocess.CompletedProcess[str]:
        captured.update(
            {
                "args": list(args),
                "check": check,
                "capture_output": capture_output,
                "text": text,
                "cwd": cwd,
                "env": dict(env),
            }
        )
        return subprocess.CompletedProcess(args=list(args), returncode=0, stdout="ok", stderr="")

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "keep-me")
    monkeypatch.setenv("DBT_TARGET", "stale")
    monkeypatch.setattr(dbt, "get_settings", lambda: settings)
    monkeypatch.setattr(dbt, "_dbt_base_command", lambda: ["dbt"])
    monkeypatch.setattr(dbt.subprocess, "run", fake_run)

    result = dbt.run_dbt_command.fn(
        command="compile",
        select=["path:models/staging"],
        exclude=[],
        project_dir="dbt",
        profiles_dir="dbt",
        target=None,
        target_path=str(tmp_path / "dbt-target"),
    )

    args = captured["args"]
    env = captured["env"]
    assert result.return_code == 0
    assert isinstance(args, list)
    assert args[:6] == [
        "dbt",
        "compile",
        "--project-dir",
        str(APP_ROOT / "dbt"),
        "--profiles-dir",
        str(APP_ROOT / "dbt"),
    ]
    assert "--target" in args
    assert args[args.index("--target") + 1] == "dev"
    assert "--target-path" in args
    assert args[args.index("--target-path") + 1] == str(tmp_path / "dbt-target")
    assert captured["cwd"] == APP_ROOT
    assert isinstance(env, dict)
    assert env["AWS_ACCESS_KEY_ID"] == "keep-me"
    assert env["DBT_TARGET"] == "dev"
    assert env["LAKE_NAME"] == "unique_stocks"
    assert env["LOCAL_LAKE_PATH"] == str(APP_ROOT / "unique_stocks.duckdb")
    assert env["DBT_DUCKDB_PATH"] == str(APP_ROOT / "unique_stocks.duckdb")


def test_run_dbt_command_passes_indirect_selection_for_build(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    """Partial deployment builds should avoid eager tests outside the selected graph."""
    settings = Settings(local_lake_path="unique_stocks.duckdb", motherduck_token=SecretStr(""))
    captured: dict[str, object] = {}

    def fake_run(
        args: Sequence[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
        cwd: Path,
        env: Mapping[str, str],
    ) -> subprocess.CompletedProcess[str]:
        captured["args"] = list(args)
        return subprocess.CompletedProcess(args=list(args), returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(dbt, "get_settings", lambda: settings)
    monkeypatch.setattr(dbt, "_dbt_base_command", lambda: ["dbt"])
    monkeypatch.setattr(dbt.subprocess, "run", fake_run)

    dbt.run_dbt_command.fn(
        command="build",
        select=["+path:models/marts/instrument"],
        exclude=[],
        indirect_selection="buildable",
        project_dir="dbt",
        profiles_dir="dbt",
        target=None,
        target_path=str(tmp_path / "dbt-target"),
    )

    args = captured["args"]
    assert isinstance(args, list)
    assert "--indirect-selection" in args
    assert args[args.index("--indirect-selection") + 1] == "buildable"


def test_run_dbt_command_enters_lake_writer_limit(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    """Dbt subprocesses should serialize against other lake writers."""
    settings = Settings(local_lake_path="unique_stocks.duckdb", motherduck_token=SecretStr(""))
    operations: list[str | None] = []

    @contextmanager
    def fake_lake_writer_limit(operation: str | None = None) -> Generator[None]:
        operations.append(operation)
        yield

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(dbt, "get_settings", lambda: settings)
    monkeypatch.setattr(dbt, "_dbt_base_command", lambda: ["dbt"])
    monkeypatch.setattr(dbt, "lake_writer_limit", fake_lake_writer_limit)
    monkeypatch.setattr(dbt.subprocess, "run", fake_run)

    dbt.run_dbt_command.fn(
        command="build",
        select=[],
        exclude=[],
        project_dir="dbt",
        profiles_dir="dbt",
        target=None,
        target_path=str(tmp_path / "dbt-target"),
    )

    assert operations == ["dbt.build"]


def test_run_dbt_command_ensures_motherduck_database_for_prod(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Prod dbt runs should create the MotherDuck database before connecting."""
    settings = Settings(motherduck_token=SecretStr("test-token"))
    ensure_calls: list[Settings] = []

    def fake_ensure(candidate: Settings) -> None:
        ensure_calls.append(candidate)

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(dbt, "get_settings", lambda: settings)
    monkeypatch.setattr(dbt, "ensure_lake_database", fake_ensure)
    monkeypatch.setattr(dbt, "_dbt_base_command", lambda: ["dbt"])
    monkeypatch.setattr(dbt.subprocess, "run", fake_run)

    dbt.run_dbt_command.fn(
        command="compile",
        select=[],
        exclude=[],
        project_dir="dbt",
        profiles_dir="dbt",
        target=None,
        target_path=str(tmp_path / "dbt-target"),
    )

    assert ensure_calls == [settings]


def test_run_dbt_command_rejects_explicit_target_conflict(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    """Explicit dbt targets should not drift from the selected lake backend."""
    monkeypatch.setattr(dbt, "get_settings", lambda: Settings(motherduck_token=SecretStr("")))

    with pytest.raises(ValueError, match="conflicts with lake backend"):
        dbt.run_dbt_command.fn(
            command="compile",
            select=[],
            exclude=[],
            project_dir="dbt",
            profiles_dir="dbt",
            target="prod",
            target_path=str(tmp_path / "dbt-target"),
        )


def test_release_local_lake_lock_skips_motherduck(monkeypatch: MonkeyPatch) -> None:
    """MotherDuck backends should not reset a local DuckDB singleton."""
    calls: list[str] = []

    def record_reset() -> None:
        calls.append("reset")

    monkeypatch.setattr(dbt, "reset_lake_client", record_reset)
    monkeypatch.setattr(dbt, "get_settings", lambda: Settings(motherduck_token=SecretStr("token")))

    dbt._release_local_lake_lock()

    assert calls == []


def test_release_local_lake_lock_resets_local_backend(monkeypatch: MonkeyPatch) -> None:
    """Local lake runs must drop cached handles before dbt opens the file."""
    calls: list[str] = []

    def record_reset() -> None:
        calls.append("reset")

    monkeypatch.setattr(dbt, "reset_lake_client", record_reset)
    monkeypatch.setattr(dbt, "get_settings", lambda: Settings(motherduck_token=SecretStr("")))

    dbt._release_local_lake_lock()

    assert calls == ["reset"]


@pytest.mark.asyncio
async def test_dbt_build_flow_releases_local_lock_before_tracker(monkeypatch: MonkeyPatch) -> None:
    """dbt-build should not open audit tracking before dropping a stale local DuckDB handle."""
    events: list[str] = []

    class FailIfConstructedTracker:
        def __init__(self) -> None:
            events.append("tracker")

    def stop_after_release() -> None:
        events.append("release")
        raise RuntimeError("stop after release")

    monkeypatch.setattr(dbt, "get_settings", lambda: Settings(motherduck_token=SecretStr("")))
    monkeypatch.setattr(dbt, "_release_local_lake_lock", stop_after_release)
    monkeypatch.setattr(dbt, "PipelineRunTracker", FailIfConstructedTracker)

    with pytest.raises(RuntimeError, match="stop after release"):
        await dbt.dbt_build_flow.fn(command="compile")

    assert events == ["release"]


@pytest.mark.asyncio
async def test_dbt_build_flow_releases_local_lock_before_subprocess(monkeypatch: MonkeyPatch) -> None:
    """dbt-build should drop its own audit connection before the dbt subprocess starts."""
    events: list[str] = []

    class FakeRun:
        run_id = "run-1"
        is_terminal = False

        def complete(self, **_: object) -> None:
            events.append("complete")
            self.is_terminal = True

        def fail(self, *_: object, **__: object) -> None:
            events.append("fail")
            self.is_terminal = True

    class FakeTracker:
        def __init__(self) -> None:
            events.append("tracker")
            self.lake = object()

        @contextmanager
        def track_run(self, **_: object) -> Generator[FakeRun]:
            events.append("track_run")
            yield FakeRun()

    def fake_release() -> None:
        events.append("release")

    def fake_run_dbt_command(**_: object) -> dbt.DbtCommandResult:
        events.append("dbt")
        return dbt.DbtCommandResult(
            command_args=["dbt", "compile"],
            return_code=0,
            stdout="",
            stderr="",
            started_at=dbt._now(),
            completed_at=dbt._now(),
            elapsed_seconds=0.0,
            artifact_path=None,
        )

    async def fake_create_artifact(**_: object) -> None:
        events.append("artifact")

    monkeypatch.setattr(dbt, "get_settings", lambda: Settings(motherduck_token=SecretStr("")))
    monkeypatch.setattr(dbt, "_release_local_lake_lock", fake_release)
    monkeypatch.setattr(dbt, "PipelineRunTracker", FakeTracker)
    monkeypatch.setattr(dbt, "run_dbt_command", fake_run_dbt_command)
    monkeypatch.setattr(dbt, "_refresh_tracker_lake", lambda _tracker: events.append("refresh"))
    monkeypatch.setattr(dbt, "read_dbt_run_results", lambda **_: None)
    monkeypatch.setattr(dbt, "_record_dbt_invocation", lambda **_: events.append("invocation"))
    monkeypatch.setattr(dbt, "_record_dbt_node_results", lambda **_: 0)
    monkeypatch.setattr(dbt, "_create_dbt_summary_artifact", fake_create_artifact)

    result = await dbt.dbt_build_flow.fn(command="compile")

    assert result["return_code"] == 0
    assert events == [
        "release",
        "tracker",
        "track_run",
        "release",
        "dbt",
        "refresh",
        "invocation",
        "artifact",
        "complete",
    ]


def test_dbt_error_message_prefixes_duckdb_lock_errors() -> None:
    """Lock failures should surface an actionable hint before the dbt traceback tail."""
    result = dbt.DbtCommandResult(
        command_args=["dbt", "build"],
        return_code=2,
        stdout="",
        stderr="IO Error: Could not set lock on file unique_stocks.duckdb",
        started_at=dbt._now(),
        completed_at=dbt._now(),
        elapsed_seconds=1.0,
        artifact_path=None,
    )

    message = dbt._dbt_error_message(result)

    assert message.startswith("DuckDB file lock conflict")


def test_read_dbt_run_results_uses_per_run_target_path(tmp_path: Path) -> None:
    """Dbt artifact reads should use the exact per-run target path."""
    target_dir = tmp_path / "target" / "pipeline-runs" / "run-1"
    target_dir.mkdir(parents=True)
    artifact = target_dir / "run_results.json"
    artifact.write_text('{"metadata": {"adapter_type": "duckdb"}, "results": []}')

    assert dbt.read_dbt_run_results.fn(target_path=str(target_dir)) == {
        "metadata": {"adapter_type": "duckdb"},
        "results": [],
    }


def test_read_dbt_run_results_ignores_stale_shared_target(tmp_path: Path) -> None:
    """A stale shared dbt target artifact must not be used for a failed invocation."""
    shared_target = tmp_path / "target"
    shared_target.mkdir()
    (shared_target / "run_results.json").write_text('{"metadata": {"adapter_type": "duckdb"}, "results": ["stale"]}')
    per_run_target = tmp_path / "target" / "pipeline-runs" / "run-1"
    per_run_target.mkdir(parents=True)

    assert dbt.read_dbt_run_results.fn(target_path=str(per_run_target)) is None


def test_run_dbt_command_uses_distinct_artifact_paths(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    """Separate invocations should report artifact paths under separate target directories."""
    settings = Settings(local_lake_path="unique_stocks.duckdb", motherduck_token=SecretStr(""))

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(dbt, "get_settings", lambda: settings)
    monkeypatch.setattr(dbt, "_dbt_base_command", lambda: ["dbt"])
    monkeypatch.setattr(dbt.subprocess, "run", fake_run)

    first = dbt.run_dbt_command.fn(
        command="compile",
        select=[],
        exclude=[],
        project_dir="dbt",
        profiles_dir="dbt",
        target=None,
        target_path=str(tmp_path / "target" / "pipeline-runs" / "run-1"),
    )
    second = dbt.run_dbt_command.fn(
        command="compile",
        select=[],
        exclude=[],
        project_dir="dbt",
        profiles_dir="dbt",
        target=None,
        target_path=str(tmp_path / "target" / "pipeline-runs" / "run-2"),
    )

    assert first.command_args[first.command_args.index("--target-path") + 1].endswith("run-1")
    assert second.command_args[second.command_args.index("--target-path") + 1].endswith("run-2")
    assert first.artifact_path is None
    assert second.artifact_path is None
