from shared.config import config
from shared.hooks.azure.datalake import AzureDataLakeHook

datalake_client = AzureDataLakeHook(config.azure.storage_account_url)
