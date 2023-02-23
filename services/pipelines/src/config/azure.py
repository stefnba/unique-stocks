from pydantic import Field
from .env import EnvBaseConfig

class AzureConfig(EnvBaseConfig):
    storage_account_url: str = Field(env="AZURE_STORAGE_ACCOUNT_URL", default=None)
