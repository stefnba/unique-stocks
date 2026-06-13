"""Application orchestration wiring.

The ``orchestration`` package contains app-specific workflow composition:
Prefect setup, dbt asset wiring, post-ingestion deployment launches, and local
smoke presets. Modules here may import from ``config``, ``domains``,
``providers``, and ``core`` because this package is the app boundary where
generic primitives are connected to concrete implementations.

Reusable infrastructure belongs in ``core``. Domain business logic belongs in
``domains``. Provider implementation details belong in ``providers``.
"""
