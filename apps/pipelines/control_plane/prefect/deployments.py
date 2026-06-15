"""Application Prefect deployment sync registration."""

from core.prefect.deployments import DEFAULT_PREFECT_YAML, PrefectDeploymentSyncBase


class PrefectDeployments(PrefectDeploymentSyncBase):
    """App-owned Prefect deployment sync configuration."""

    DEFAULT_YAML = DEFAULT_PREFECT_YAML
    """Default Prefect project manifest path."""


__all__ = ["PrefectDeployments"]
