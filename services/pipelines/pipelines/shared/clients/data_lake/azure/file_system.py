from adlfs import AzureBlobFileSystem
from shared.config import CONFIG

# Filesystem interface to Azure DataLake Gen2 Storage
abfs_client = AzureBlobFileSystem(account_name=CONFIG.azure.account_name, anon=False)
