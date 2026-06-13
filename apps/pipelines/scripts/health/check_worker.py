"""CLI entrypoint for container-local Prefect worker health checks."""

from __future__ import annotations

from core.operations.worker_health import main

if __name__ == "__main__":
    raise SystemExit(main())
