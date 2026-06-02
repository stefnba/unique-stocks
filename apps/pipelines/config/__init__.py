"""App-level configuration: settings and Prefect block registry."""

from config.blocks import BlockRegistry
from config.settings import APP_ROOT, SETTINGS, Settings, get_settings

__all__ = [
    "APP_ROOT",
    "BlockRegistry",
    "SETTINGS",
    "Settings",
    "get_settings",
]
