"""Tests for Prefect deployment sync helpers."""

from uuid import UUID

import pytest

from config.settings import APP_ROOT
from control_plane.prefect.dbt_builds import DBT_BUILD_SPECS
from core.orchestration.deployments import (
    DEFAULT_PREFECT_YAML,
    DeploymentKey,
    expected_deployment_keys,
    find_orphaned_deployments,
    is_managed_entrypoint,
    load_prefect_yaml_deployments,
    resolve_flow_name,
)

APPS_PIPELINES = APP_ROOT


def test_load_prefect_yaml_deployments() -> None:
    """The repo manifest should declare the current deployment set."""
    deployments = load_prefect_yaml_deployments(APPS_PIPELINES / DEFAULT_PREFECT_YAML)
    assert len(deployments) == 20


def test_expected_deployment_keys_matches_manifest() -> None:
    """Expected keys should include the canonical eod and dbt deployment slugs."""
    deployments = load_prefect_yaml_deployments(APPS_PIPELINES / DEFAULT_PREFECT_YAML)
    keys = expected_deployment_keys(deployments)

    assert DeploymentKey("eod-price-daily", "eod-price-refresh-daily") in keys
    assert DeploymentKey("dbt-build", "instrument-build") in keys
    assert DeploymentKey("dbt-build", "fundamental-build") in keys
    assert DeploymentKey("exchange-reference-refresh", "exchange-reference-refresh-monthly") in keys
    assert DeploymentKey("exchange-schedule-refresh", "exchange-schedule-refresh-weekly") in keys
    assert DeploymentKey("fundamental-quarterly", "fundamental-refresh-quarterly") in keys
    assert DeploymentKey("fundamental-quarterly", "fundamental-replay") in keys
    assert len(keys) == 20


def test_ingestion_deployments_enable_clean_post_ingestion_builds() -> None:
    """Production-facing ingestion deployments should keep Silver/Gold fresh."""
    deployments = load_prefect_yaml_deployments(APPS_PIPELINES / DEFAULT_PREFECT_YAML)
    by_key = {
        DeploymentKey(resolve_flow_name(deployment), str(deployment["name"])): deployment for deployment in deployments
    }

    for key in (
        DeploymentKey("eod-price-daily", "eod-price-refresh-daily"),
        DeploymentKey("eod-price-daily", "eod-price-backfill"),
        DeploymentKey("eod-price-backfill", "eod-price-historical-backfill"),
        DeploymentKey("exchange-schedule-refresh", "exchange-schedule-refresh-manual"),
        DeploymentKey("exchange-schedule-refresh", "exchange-schedule-refresh-weekly"),
        DeploymentKey("instrument-refresh", "instrument-refresh-weekly"),
        DeploymentKey("instrument-refresh", "instrument-refresh-manual"),
        DeploymentKey("fundamental-quarterly", "fundamental-refresh-manual"),
        DeploymentKey("fundamental-quarterly", "fundamental-refresh-quarterly"),
        DeploymentKey("fundamental-quarterly", "fundamental-backfill"),
        DeploymentKey("fundamental-quarterly", "fundamental-replay"),
    ):
        parameters = by_key[key].get("parameters")
        assert isinstance(parameters, dict)
        assert parameters["run_dbt_build"] is True

    historical_backfill_parameters = by_key[DeploymentKey("eod-price-backfill", "eod-price-historical-backfill")].get(
        "parameters"
    )
    assert isinstance(historical_backfill_parameters, dict)
    assert historical_backfill_parameters["batch_size"] == 100
    assert historical_backfill_parameters["build_selection_views_if_missing"] is True


def test_dbt_build_specs_match_manifest_selectors() -> None:
    """Inline child dbt builds should use the same selectors as direct deployments."""
    deployments = load_prefect_yaml_deployments(APPS_PIPELINES / DEFAULT_PREFECT_YAML)
    by_key = {
        DeploymentKey(resolve_flow_name(deployment), str(deployment["name"])): deployment for deployment in deployments
    }

    for build, spec in DBT_BUILD_SPECS.items():
        parameters = by_key[DeploymentKey("dbt-build", build)].get("parameters")
        assert isinstance(parameters, dict)
        assert parameters["select"] == list(spec.select)


@pytest.mark.parametrize(
    ("entrypoint", "expected"),
    [
        ("domains.eod_price.flows:eod_price_flow", False),
        ("orchestration.flows.eod_price:eod_price_flow", True),
        ("orchestration.flows.instrument:instrument_flow", True),
        ("orchestration.dbt:dbt_build_flow", True),
        ("some.other.module:flow_fn", False),
        (None, False),
    ],
)
def test_is_managed_entrypoint(entrypoint: str | None, expected: bool) -> None:
    """Managed entrypoint detection should scope pruning to this app."""
    assert is_managed_entrypoint(entrypoint) is expected


def test_find_orphaned_deployments_only_prunes_managed_entries() -> None:
    """Orphans should exclude yaml entries and unrelated UI deployments."""
    expected = {DeploymentKey("eod-price-daily", "eod-price-refresh-daily")}
    delete_id = UUID("00000000-0000-0000-0000-000000000001")
    keep_id = UUID("00000000-0000-0000-0000-000000000002")
    ignore_id = UUID("00000000-0000-0000-0000-000000000003")
    legacy_id = UUID("00000000-0000-0000-0000-000000000004")

    class _Deployment:
        def __init__(self, deployment_id: UUID, entrypoint: str | None) -> None:
            self.id = deployment_id
            self.entrypoint = entrypoint

    server = [
        (
            DeploymentKey("eod-price-daily", "eod-price-refresh-daily"),
            _Deployment(keep_id, "orchestration.flows.eod_price:eod_price_flow"),
        ),
        (
            DeploymentKey("eod-price-daily", "legacy-backfill"),
            _Deployment(delete_id, "orchestration.flows.eod_price:eod_price_flow"),
        ),
        (
            DeploymentKey("eod-price-daily", "old-domain-backfill"),
            _Deployment(legacy_id, "domains.eod_price.flows:eod_price_flow"),
        ),
        (
            DeploymentKey("experimental-flow", "try"),
            _Deployment(ignore_id, "experiments.flows:try_flow"),
        ),
    ]

    orphans = find_orphaned_deployments(expected=expected, server=server)

    assert [orphan.key.slug for orphan in orphans] == ["eod-price-daily/legacy-backfill"]
    assert orphans[0].deployment_id == delete_id
