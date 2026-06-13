"""Sync Prefect deployments with ``prefect.yaml``.

This module makes the checked-in deployment manifest the source of truth:

1. Remove orphaned deployments that belong to this app (entrypoints under
   ``domains.`` or ``core.transforms.``) but are no longer declared in yaml.
2. Run ``prefect deploy --all`` to create or update the declared deployments.

Unrelated deployments created manually in the Prefect UI are left untouched.
"""

from __future__ import annotations

import importlib
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import UUID

import structlog
import yaml
from prefect.client.orchestration import get_client
from prefect.client.schemas.responses import DeploymentResponse

log = structlog.get_logger(__name__)

MANAGED_ENTRYPOINT_PREFIXES: tuple[str, ...] = ("domains.", "core.transforms.")
DEFAULT_PREFECT_YAML = Path("prefect.yaml")


class ReadableDeployment(Protocol):
    """Minimal deployment surface needed for orphan detection."""

    id: UUID
    entrypoint: str | None


@dataclass(frozen=True, slots=True)
class DeploymentKey:
    """Stable identity for a Prefect deployment."""

    flow_name: str
    deployment_name: str

    @property
    def slug(self) -> str:
        """Return the canonical ``flow/deployment`` identifier."""
        return f"{self.flow_name}/{self.deployment_name}"


@dataclass(frozen=True, slots=True)
class OrphanedDeployment:
    """Server deployment that should be removed during sync."""

    key: DeploymentKey
    deployment_id: UUID
    entrypoint: str | None


def load_prefect_yaml_deployments(path: Path) -> list[dict[str, object]]:
    """Return the deployment list from a Prefect project manifest."""
    data = yaml.safe_load(path.read_text())
    deployments = data.get("deployments")
    if not isinstance(deployments, list):
        raise ValueError(f"{path} is missing a top-level 'deployments' list.")
    return deployments


def resolve_flow_name(deployment: dict[str, object]) -> str:
    """Resolve the Prefect flow name for one yaml deployment entry."""
    flow_name = deployment.get("flow_name")
    if isinstance(flow_name, str) and flow_name:
        return flow_name

    entrypoint = deployment.get("entrypoint")
    if not isinstance(entrypoint, str) or ":" not in entrypoint:
        raise ValueError(f"Deployment {deployment.get('name')!r} is missing a resolvable entrypoint.")

    module_path, function_name = entrypoint.rsplit(":", 1)
    module = importlib.import_module(module_path)
    flow_object = getattr(module, function_name)
    name = getattr(flow_object, "name", None)
    if not isinstance(name, str) or not name:
        raise ValueError(f"Entrypoint {entrypoint!r} does not expose a Prefect flow name.")
    return name


def expected_deployment_keys(deployments: Sequence[dict[str, object]]) -> set[DeploymentKey]:
    """Build the set of deployment identities declared in ``prefect.yaml``."""
    keys: set[DeploymentKey] = set()
    for deployment in deployments:
        name = deployment.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(f"Each deployment must have a non-empty name: {deployment!r}")
        keys.add(
            DeploymentKey(
                flow_name=resolve_flow_name(deployment),
                deployment_name=name,
            )
        )
    return keys


def is_managed_entrypoint(entrypoint: str | None) -> bool:
    """Return whether a server deployment belongs to this pipelines app."""
    if not entrypoint:
        return False
    return entrypoint.startswith(MANAGED_ENTRYPOINT_PREFIXES)


def find_orphaned_deployments(
    *,
    expected: set[DeploymentKey],
    server: Sequence[tuple[DeploymentKey, ReadableDeployment]],
) -> list[OrphanedDeployment]:
    """Return managed server deployments that are absent from the yaml manifest."""
    orphans: list[OrphanedDeployment] = []
    for key, deployment in server:
        if key in expected:
            continue
        if not is_managed_entrypoint(deployment.entrypoint):
            continue
        orphans.append(
            OrphanedDeployment(
                key=key,
                deployment_id=deployment.id,
                entrypoint=deployment.entrypoint,
            )
        )
    return sorted(orphans, key=lambda item: item.key.slug)


async def _read_server_deployments() -> list[tuple[DeploymentKey, DeploymentResponse]]:
    """Load all deployments currently registered on the Prefect server."""
    rows: list[tuple[DeploymentKey, DeploymentResponse]] = []
    async with get_client() as client:
        deployments = await client.read_deployments(limit=200)
        for deployment in deployments:
            flow = await client.read_flow(deployment.flow_id)
            rows.append(
                (
                    DeploymentKey(flow_name=flow.name, deployment_name=deployment.name),
                    deployment,
                )
            )
    return rows


async def _delete_orphans(orphans: Sequence[OrphanedDeployment], *, dry_run: bool) -> None:
    """Delete orphaned deployments from the Prefect server."""
    if not orphans:
        log.info("deploy.sync.no_orphans")
        return

    for orphan in orphans:
        if dry_run:
            log.info(
                "deploy.sync.would_delete_orphan",
                deployment=orphan.key.slug,
                entrypoint=orphan.entrypoint,
            )
            continue

        async with get_client() as client:
            await client.delete_deployment(orphan.deployment_id)
        log.info("deploy.sync.deleted_orphan", deployment=orphan.key.slug)


def _run_prefect_deploy(*, dry_run: bool) -> None:
    """Apply the yaml manifest with Prefect's deploy command."""
    if dry_run:
        log.info("deploy.sync.would_apply_manifest")
        return

    subprocess.run(
        ["prefect", "deploy", "--all", "--no-prompt"],
        check=True,
    )


async def sync_deployments(
    *,
    prefect_yaml: Path,
    dry_run: bool,
    prune_only: bool,
) -> int:
    """Prune orphaned app deployments and apply ``prefect.yaml``."""
    deployments = load_prefect_yaml_deployments(prefect_yaml)
    expected = expected_deployment_keys(deployments)
    server = await _read_server_deployments()
    orphans = find_orphaned_deployments(expected=expected, server=server)

    log.info(
        "deploy.sync.plan",
        manifest=str(prefect_yaml),
        expected=len(expected),
        server=len(server),
        orphans=len(orphans),
        dry_run=dry_run,
        prune_only=prune_only,
    )

    for key in sorted(expected, key=lambda item: item.slug):
        log.info("deploy.sync.manifest_entry", deployment=key.slug)

    await _delete_orphans(orphans, dry_run=dry_run)

    if not prune_only:
        _run_prefect_deploy(dry_run=dry_run)

    return 0
