"""Shared infrastructure for the pipelines app.

The ``core`` package contains reusable plumbing that is intentionally not tied
to one business domain:

- reusable infrastructure capabilities such as HTTP, object storage, and the lake
- HTTP and lake base model contracts used by providers and domains
- ingestion helpers that turn parser output into landing objects and Bronze rows
- small orchestration utilities and environment helpers

Domain packages decide *what* to ingest. ``core`` decides *how* common
infrastructure behavior is expressed consistently.
"""

from core.utils.logging import configure_logging

configure_logging()
