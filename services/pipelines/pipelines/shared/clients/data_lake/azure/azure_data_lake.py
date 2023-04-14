from shared.config import CONFIG
from shared.hooks.azure.data_lake import AzureDatalakeHook

dl_client = AzureDatalakeHook(account_url=CONFIG.azure.storage_account_url, file_system=CONFIG.azure.file_system)
