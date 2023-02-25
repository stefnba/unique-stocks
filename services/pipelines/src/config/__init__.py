"""
Read more on configuration here
https://rednafi.github.io/digressions/python/2020/06/03/python-configs.html
"""

from pydantic import BaseModel

from .app import AppConfig
from .azure import AzureConfig


class GlobalConfig(BaseModel):
    """
    Global configurations.
    """

    app = AppConfig()
    azure = AzureConfig()


config = GlobalConfig()

__all__ = ["config"]
