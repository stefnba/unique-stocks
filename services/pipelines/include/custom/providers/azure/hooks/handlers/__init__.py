from custom.providers.azure.hooks.handlers.read.local import LocalReadHandlers
from custom.providers.azure.hooks.handlers.read.azure import AzureReadHandlers
from custom.providers.azure.hooks.handlers.write.azure import AzureWriteHandlers
from custom.providers.azure.hooks.handlers.write.local import LocalWriteHandlers


class DatasetReadHandlers:
    Azure = AzureReadHandlers
    Local = LocalReadHandlers


class DatasetWriteHandlers:
    Azure = AzureWriteHandlers
    Local = LocalWriteHandlers


class DatasetHandlers:
    Read = DatasetReadHandlers
    Write = DatasetWriteHandlers


__all__ = ["DatasetHandlers"]
