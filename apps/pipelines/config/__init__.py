"""App-level configuration: settings and Prefect block registry."""

from config.blocks import BlockRegistry
from config.settings import SETTINGS, Settings, get_settings

__all__ = ["BlockRegistry", "SETTINGS", "Settings", "get_settings"]
