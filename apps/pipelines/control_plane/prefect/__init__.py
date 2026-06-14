"""App-specific Prefect control-plane wiring."""

from control_plane.prefect.blocks import BlockRegistry
from control_plane.prefect.controls import PrefectControls
from control_plane.prefect.deployments import PrefectDeployments

__all__ = ["BlockRegistry", "PrefectControls", "PrefectDeployments"]
