"""Tests for the container-local worker health check."""

from pathlib import Path

from core.operations import worker_health


def test_worker_process_is_running_detects_prefect_worker(tmp_path: Path) -> None:
    """Procfs scanning should detect a sibling Prefect worker process."""
    worker_proc = tmp_path / "123"
    worker_proc.mkdir()
    (worker_proc / "cmdline").write_bytes(b"uv\x00run\x00prefect\x00worker\x00start\x00--pool\x00default")

    assert worker_health.worker_process_is_running(tmp_path) is True


def test_worker_process_is_running_ignores_non_worker_processes(tmp_path: Path) -> None:
    """Unrelated processes should not satisfy the worker health check."""
    shell_proc = tmp_path / "456"
    shell_proc.mkdir()
    (shell_proc / "cmdline").write_bytes(b"sh\x00-lc\x00sleep\x0030")

    assert worker_health.worker_process_is_running(tmp_path) is False
