"""CLI entrypoint for production-facing operational health checks."""

from __future__ import annotations

from core.operations.operational_health import main

if __name__ == "__main__":
    raise SystemExit(main())
