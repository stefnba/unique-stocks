"""Shared infrastructure for the pipelines app.

The ``core`` package contains reusable plumbing that is intentionally not tied
to one business domain:

- clients for external infrastructure such as S3 and the lake
- base model contracts used by providers and domains
- ingestion helpers that turn parser output into landing objects and Bronze rows
- small orchestration utilities and environment helpers

Domain packages decide *what* to ingest. ``core`` decides *how* common
infrastructure behavior is expressed consistently.
"""
