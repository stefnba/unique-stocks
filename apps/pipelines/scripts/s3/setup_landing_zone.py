"""CLI entrypoint for S3 landing-zone provisioning."""

from __future__ import annotations

from core.clients.storage.s3.provisioning import main

if __name__ == "__main__":
    raise SystemExit(main())
