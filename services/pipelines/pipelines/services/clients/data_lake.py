from services.hooks.azure.data_lake import AzureDataLakeClient
from services.config import config

datalake_client = AzureDataLakeClient(config.azure.storage_account_url)
