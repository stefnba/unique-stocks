from airflow.hooks.base import BaseHook
from custom.providers.azure.hooks.types import AzureDataLakeCredentials
from settings import config_settings
from typing import Optional, Literal
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential


class AzureDataLakeStorageHook(BaseHook):
    """
    Integration with Azure Blob Storage.

    It communicate via the Window Azure Storage Blob protocol. Contrary to the Airflow Azure WASB hook, it can also
    utilize a `adls` connection.
    """

    conn_id: str
    credentials: AzureDataLakeCredentials
    blob_service_client: BlobServiceClient

    def __init__(
        self, conn_id: str, account_name: Optional[str] = None, default_container: Optional[str] = None
    ) -> None:
        self.conn_id = conn_id

        account_name = account_name or config_settings.azure.account_name
        if not account_name:
            raise Exception('"account_name" needs to be specified.')

        self.credentials = self.get_conn()

        self.container = default_container

        self.blob_service_client = BlobServiceClient(
            account_url=self.credentials.account_url,
            credential=ClientSecretCredential(
                client_id=self.credentials.client_id,
                client_secret=self.credentials.secret,
                tenant_id=self.credentials.tenant_id,
            ),
        )

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson or {}

        tenant = extra.get("tenant_id")

        if not tenant:
            raise ValueError('"tenant_id" is missing.')

        # use Active Directory auth
        account_name = conn.host
        client = conn.login
        secret = conn.password

        account_url = f"https://{conn.host}.blob.core.windows.net/"

        return AzureDataLakeCredentials(
            tenant_id=tenant,
            secret=secret,
            account_name=account_name,
            client_id=client,
            account_url=account_url,
        )

    def download(
        self,
        blob_path: str,
        container: Optional[str] = None,
        offset: int | None = None,
        length: int | None = None,
        **kwargs,
    ):
        """Downloads a blob to the StorageStreamDownloader."""
        container = container or self.container

        if not container:
            raise Exception('"container" must be specified.')

        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob_path)
        if offset:
            kwargs["offset"] = offset
        if length:
            kwargs["length"] = length

        return blob_client.download_blob(**kwargs, encoding=None)

    def read_blob(
        self,
        blob_path: str,
        container: Optional[str] = None,
    ):
        """Read a file from Azure Blob Storage and return as a bytes."""
        return self.download(blob_path=blob_path, container=container).readall()

    def upload(
        self,
        container: str,
        blob_path: str,
        data: str | bytes,
        blob_type: Literal["BlockBlob", "PageBlob", "AppendBlob"] = "BlockBlob",
        length: int | None = None,
        overwrite=False,
        **kwargs,
    ) -> str:
        """
        Creates a new blob from a data source with automatic chunking.
        """

        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob_path)
        blob_client.upload_blob(data=data, overwrite=overwrite)

        return f"{container}/{blob_path}"
