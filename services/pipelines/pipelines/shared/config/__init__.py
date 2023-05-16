"""
Read more on configuration here
https://rednafi.github.io/digressions/python/2020/06/03/python-configs.html
"""

from pydantic import BaseModel
from shared.config.api_keys import ApiKeys
from shared.config.app import AppConfig, DataConfig
from shared.config.azure import AzureConfig
from shared.config.data_lake import DataLakeConfig


class GlobalConfig(BaseModel):
    """
    Global configurations.
    """

    api_keys = ApiKeys()
    app = AppConfig()
    azure = AzureConfig()
    data_lake = DataLakeConfig()
    data = DataConfig()


CONFIG = GlobalConfig()

__all__ = ["CONFIG"]
