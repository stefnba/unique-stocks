"""Tests for Prefect deployment sync helpers."""

from pathlib import Path
from uuid import UUID

import pytest

from scripts.sync_prefect_deployments import (
    DEFAULT_PREFECT_YAML,
    DeploymentKey,
    expected_deployment_keys,
    find_orphaned_deployments,
    is_managed_entrypoint,
    load_prefect_yaml_deployments,
    resolve_flow_name,
)

APPS_PIPELINES = Path(__file__).resolve().parents[2]


def test_load_prefect_yaml_deployments() -> None:
    """The repo manifest should declare the current deployment set."""
    deployments = load_prefect_yaml_deployments(APPS_PIPELINES / DEFAULT_PREFECT_YAML)
    assert len(deployments) == 15


def test_expected_deployment_keys_matches_manifest() -> None:
    """Expected keys should include the canonical eod and dbt deployment slugs."""
    deployments = load_prefect_yaml_deployments(APPS_PIPELINES / DEFAULT_PREFECT_YAML)
    keys = expected_deployment_keys(deployments)

    assert DeploymentKey("eod-price-daily", "daily") in keys
    assert DeploymentKey("dbt-build", "instrument-build") in keys
    assert DeploymentKey("dbt-build", "fundamental-build") in keys
    assert DeploymentKey("fundamental-quarterly", "replay") in keys
    assert len(keys) == 15


def test_ingestion_deployments_enable_clean_post_ingestion_builds() -> None:
    """Production-facing ingestion deployments should keep Silver/Gold fresh."""
    deployments = load_prefect_yaml_deployments(APPS_PIPELINES / DEFAULT_PREFECT_YAML)
    by_key = {
        DeploymentKey(resolve_flow_name(deployment), str(deployment["name"])): deployment for deployment in deployments
    }

    for key in (
        DeploymentKey("eod-price-daily", "daily"),
        DeploymentKey("eod-price-daily", "backfill"),
        DeploymentKey("eod-price-backfill", "historical-backfill"),
        DeploymentKey("exchange-schedule-refresh", "manual"),
        DeploymentKey("instrument-refresh", "weekly"),
        DeploymentKey("instrument-refresh", "manual"),
        DeploymentKey("fundamental-quarterly", "manual"),
        DeploymentKey("fundamental-quarterly", "backfill"),
        DeploymentKey("fundamental-quarterly", "replay"),
    ):
        parameters = by_key[key].get("parameters")
        assert isinstance(parameters, dict)
        assert parameters["run_dbt_build"] is True


@pytest.mark.parametrize(
    ("entrypoint", "expected"),
    [
        ("domains.eod_price.flows:eod_price_flow", True),
        ("core.transforms.dbt:dbt_build_flow", True),
        ("some.other.module:flow_fn", False),
        (None, False),
    ],
)
def test_is_managed_entrypoint(entrypoint: str | None, expected: bool) -> None:
    """Managed entrypoint detection should scope pruning to this app."""
    assert is_managed_entrypoint(entrypoint) is expected


def test_find_orphaned_deployments_only_prunes_managed_entries() -> None:
    """Orphans should exclude yaml entries and unrelated UI deployments."""
    expected = {DeploymentKey("eod-price-daily", "daily")}
    delete_id = UUID("00000000-0000-0000-0000-000000000001")
    keep_id = UUID("00000000-0000-0000-0000-000000000002")
    ignore_id = UUID("00000000-0000-0000-0000-000000000003")

    class _Deployment:
        def __init__(self, deployment_id: UUID, entrypoint: str | None) -> None:
            self.id = deployment_id
            self.entrypoint = entrypoint

    server = [
        (DeploymentKey("eod-price-daily", "daily"), _Deployment(keep_id, "domains.eod_price.flows:eod_price_flow")),
        (
            DeploymentKey("eod-price-daily", "legacy-backfill"),
            _Deployment(delete_id, "domains.eod_price.flows:eod_price_flow"),
        ),
        (
            DeploymentKey("experimental-flow", "try"),
            _Deployment(ignore_id, "experiments.flows:try_flow"),
        ),
    ]

    orphans = find_orphaned_deployments(expected=expected, server=server)

    assert [orphan.key.slug for orphan in orphans] == ["eod-price-daily/legacy-backfill"]
    assert orphans[0].deployment_id == delete_id
