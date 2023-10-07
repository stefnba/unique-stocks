from airflow.hooks.base import BaseHook
from custom.providers.azure.hooks.types import AzureDataLakeCredentials
from typing import Literal
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from utils.filesystem.path import PathInput, LocalPath, AdlsPath
from utils.file.stream import read_large_file
from shared.types import DataLakeZone


class AzureDataLakeStorageHook(BaseHook):
    """
    Integration with Azure Blob Storage.

    It communicate via the Window Azure Storage Blob protocol. Contrary to the Airflow Azure WASB hook, it can also
    utilize a `adls` connection.
    """

    conn_id: str
    credentials: AzureDataLakeCredentials
    blob_service_client: BlobServiceClient

    def __init__(self, conn_id: str) -> None:
        self.conn_id = conn_id

        self.credentials = self.get_conn()

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
        blob: str,
        container: DataLakeZone,
        offset: int | None = None,
        length: int | None = None,
        **kwargs,
    ):
        """Downloads a blob to the StorageStreamDownloader."""

        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)
        if offset:
            kwargs["offset"] = offset
        if length:
            kwargs["length"] = length

        return blob_client.download_blob(**kwargs, encoding=None)

    def read_blob(
        self,
        blob: str,
        container: DataLakeZone,
    ):
        """Read a file from Azure Blob Storage and return as a bytes."""
        return self.download(blob=blob, container=container).readall()

    def upload(
        self,
        container: DataLakeZone,
        blob: str,
        data: str | bytes,
        blob_type: Literal["BlockBlob", "PageBlob", "AppendBlob"] = "BlockBlob",
        length: int | None = None,
        overwrite=False,
        **kwargs,
    ) -> str:
        """
        Creates a new blob from a data source with automatic chunking.
        """

        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)
        blob_client.upload_blob(data=data, overwrite=overwrite)

        return f"{container}/{blob}"

    def upload_from_url(
        self,
        url: str,
        container: DataLakeZone,
        blob: str,
    ):
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)
        blob_client.upload_blob_from_url(source_url=url)

    def upload_file(
        self,
        container: DataLakeZone,
        blob: str,
        file_path: PathInput,
        stream=False,
        overwrite=False,
        chunk_size=4 * 1024 * 1024,
        **kwargs,
    ) -> AdlsPath:
        """`
        Upload a local file to a blob.
        """

        file_path = LocalPath.create(file_path)

        if stream:
            # read file content in chunks and upload each chunk
            blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)

            blob_client.upload_blob(
                data=read_large_file(file_path=file_path, chunk_size=chunk_size),
            )

        else:
            # read entire file content and upload to blob
            with open(file_path.uri, "rb") as file:
                self.upload(container=container, blob=blob, data=file.read())

        return AdlsPath(container=container, blob=blob)

    def stream_to_local_file(
        self,
        file_path: PathInput,
        container: DataLakeZone,
        blob: str,
    ):
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)

        file_path = LocalPath.create(file_path)
        file_path.create_dir()  # create all dirs if they don't exist yet

        stream = blob_client.download_blob()

        with open(file=file_path.uri, mode="wb") as f:
            for chunk in stream.chunks():
                f.write(chunk)

    def remove_container(self, container: DataLakeZone):
        """Delete a container."""
        self.blob_service_client.delete_container(container=container)

    def create_container(self, container: DataLakeZone):
        """Create a container."""
        self.blob_service_client.create_container(name=container)
