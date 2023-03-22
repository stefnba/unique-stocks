from shared.config import config
from shared.hooks.azure.datalake import AzureDatalakeHook

datalake_client = AzureDatalakeHook(account_url=config.azure.storage_account_url, file_system=config.azure.file_system)
