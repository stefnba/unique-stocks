"""App-specific Prefect control-plane wiring."""

from control_plane.prefect.blocks import BlockRegistry
from control_plane.prefect.deployments import PrefectDeployments

__all__ = ["BlockRegistry", "PrefectDeployments"]
