from services.hooks.azure.data_lake import AzureDataLakeHook
from services.config import config

datalake_client = AzureDataLakeHook(config.azure.storage_account_url)
