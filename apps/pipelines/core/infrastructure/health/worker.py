"""Container-local worker health check.

This check is intentionally narrow: it verifies that the worker container can
reach the Prefect API and that a Prefect worker process is running in the same
container. End-to-end ingestion health belongs in ``operational_health``.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

from core.operations.health_common import prefect_api_is_healthy

type PrefectApiHealthCheck = Callable[[str], bool]


def worker_process_is_running(proc_root: Path = Path("/proc")) -> bool:
    """Return whether a Prefect worker process is visible in procfs."""
    if not proc_root.exists():
        return False

    current_pid = str(os.getpid())
    for pid_dir in proc_root.iterdir():
        if not pid_dir.name.isdigit() or pid_dir.name == current_pid:
            continue
        cmdline_path = pid_dir / "cmdline"
        try:
            cmdline = cmdline_path.read_bytes().replace(b"\x00", b" ").decode(errors="ignore").lower()
        except OSError:
            continue
        if "prefect" in cmdline and "worker" in cmdline and "start" in cmdline:
            return True
    return False


def check_worker_health(
    *,
    api_url: str,
    proc_root: Path = Path("/proc"),
    api_health_check: PrefectApiHealthCheck = prefect_api_is_healthy,
) -> list[str]:
    """Return worker health failures for the given runtime inputs."""
    if not api_url:
        return ["PREFECT_API_URL is not set"]
    if not api_health_check(api_url):
        return ["Prefect API health endpoint is not reachable"]
    if not worker_process_is_running(proc_root):
        return ["Prefect worker process is not running"]
    return []
