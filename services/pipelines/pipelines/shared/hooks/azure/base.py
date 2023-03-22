from typing import Optional

from azure.identity import DefaultAzureCredential


class AzureBaseClient:
    """
    Base Client for Azure that handles authentification.
    """

    account_url: str
    file_system: Optional[str]  # container

    def __init__(self, account_url: str, file_system: Optional[str] = None) -> None:
        self.account_url = account_url
        self.file_system = file_system
        self.init()

    def init(self):
        pass

    def auth(self):
        """
        Authenticates application through DefaultAzureCredential.
        """
        credential = DefaultAzureCredential()
        return credential
