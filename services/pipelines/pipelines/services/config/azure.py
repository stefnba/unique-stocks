from pydantic import Field

from services.config.env import EnvBaseConfig


class AzureConfig(EnvBaseConfig):
    storage_account_url: str = Field(env="AZURE_STORAGE_ACCOUNT_URL", default=None)
    file_system = "dev"
