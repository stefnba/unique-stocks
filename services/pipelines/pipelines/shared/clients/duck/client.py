from shared.clients.datalake.azure.file_system import abfs_client
from shared.hooks.duck.hook import DuckDbHook

duck = DuckDbHook(file_system=abfs_client)