"""
Read more on configuration here
https://rednafi.github.io/digressions/python/2020/06/03/python-configs.html
"""

from pydantic import BaseModel
from shared.config.api_keys import ApiKeys
from shared.config.app import AppConfig
from shared.config.azure import AzureConfig
from shared.config.datalake import DatalakeConfig


class GlobalConfig(BaseModel):
    """
    Global configurations.
    """

    api_keys = ApiKeys()
    app = AppConfig()
    azure = AzureConfig()
    datalake = DatalakeConfig()


config = GlobalConfig()

__all__ = ["config"]
