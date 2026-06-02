"""Tests for dbt flow runtime configuration."""

from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest
from pydantic import SecretStr
from pytest import MonkeyPatch

from config.settings import APP_ROOT, Settings
from core.transforms import dbt


def test_run_dbt_command_uses_app_root_paths_and_env_overlay(
    monkeypatch: MonkeyPatch,
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
    assert captured["cwd"] == APP_ROOT
    assert isinstance(env, dict)
    assert env["AWS_ACCESS_KEY_ID"] == "keep-me"
    assert env["DBT_TARGET"] == "dev"
    assert env["DBT_DUCKDB_PATH"] == str(APP_ROOT / "unique_stocks.duckdb")


def test_run_dbt_command_rejects_explicit_target_conflict(monkeypatch: MonkeyPatch) -> None:
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
        )


def test_read_dbt_run_results_uses_resolved_project_path(tmp_path: Path) -> None:
    """Dbt artifact reads should use the resolved project path."""
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    artifact = target_dir / "run_results.json"
    artifact.write_text('{"metadata": {"adapter_type": "duckdb"}, "results": []}')

    assert dbt.read_dbt_run_results.fn(project_dir=str(tmp_path)) == {
        "metadata": {"adapter_type": "duckdb"},
        "results": [],
    }
