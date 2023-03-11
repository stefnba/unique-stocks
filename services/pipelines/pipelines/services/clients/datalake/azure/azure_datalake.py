from services.hooks.azure.datalake import AzureDataLakeHook
from services.config import config

datalake_client = AzureDataLakeHook(config.azure.storage_account_url)
