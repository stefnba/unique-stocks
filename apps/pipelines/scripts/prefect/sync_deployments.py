"""CLI entrypoint for syncing Prefect deployments."""

from __future__ import annotations

from core.prefect.deployments import main

if __name__ == "__main__":
    raise SystemExit(main())
