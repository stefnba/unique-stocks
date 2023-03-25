from adlfs import AzureBlobFileSystem
from shared.config import config

# Filesystem interface to Azure Datalake Gen2 Storage
abfs_client = AzureBlobFileSystem(account_name=config.azure.account_name, anon=False)
