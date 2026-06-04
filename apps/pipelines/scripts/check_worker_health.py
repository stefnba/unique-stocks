"""Container-local worker health check.

This check is intentionally narrow: it verifies that the worker container can
reach the Prefect API and that a Prefect worker process is running in the same
container. End-to-end ingestion health belongs in ``check_operational_health``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from scripts.health_common import prefect_api_is_healthy


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


def main() -> int:
    """Run the worker health check and return a process exit code."""
    api_url = os.environ.get("PREFECT_API_URL", "")
    if not api_url:
        print("PREFECT_API_URL is not set", file=sys.stderr)
        return 1
    if not prefect_api_is_healthy(api_url):
        print("Prefect API health endpoint is not reachable", file=sys.stderr)
        return 1
    if not worker_process_is_running():
        print("Prefect worker process is not running", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
