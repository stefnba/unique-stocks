from include.config import config

from include.services.azure.datalake_client import AzureDataLakeClient

datalake_client = AzureDataLakeClient(config.azure.storage_account_url)


__all__ = ["datalake_client"]
