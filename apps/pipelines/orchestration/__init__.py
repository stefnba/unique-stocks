"""Application orchestration wiring.

The ``orchestration`` package contains app-specific workflow composition:
thin Prefect flow entrypoints, dbt asset/build mappings, post-ingestion
deployment launches, and local flow-check presets. Modules here may import from
``config``, ``domains``, ``providers``, and ``core`` because this package is the
app boundary where generic primitives are connected to concrete implementations.

Runtime control-plane setup belongs in ``control_plane``. Reusable machinery
belongs in ``core``. Domain business logic belongs in ``domains``. Provider
implementation details belong in ``providers``.
"""
