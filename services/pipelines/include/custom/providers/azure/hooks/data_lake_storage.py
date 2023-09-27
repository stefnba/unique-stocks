from airflow.hooks.base import BaseHook
from custom.providers.azure.hooks.types import AzureDataLakeCredentials
from typing import Literal
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
        container: str,
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
        container: str,
    ):
        """Read a file from Azure Blob Storage and return as a bytes."""
        return self.download(blob=blob, container=container).readall()

    def upload(
        self,
        container: str,
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
        container: str,
        blob: str,
    ):
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)
        blob_client.upload_blob_from_url(source_url=url)

    def upload_file(
        self,
        container: str,
        blob: str,
        file_path: str,
        stream=False,
        overwrite=False,
        **kwargs,
    ):
        """
        Upload a local file to a blob.

        Args:
            container (str): _description_
            blob (str): _description_
            file_path (str): _description_
            overwrite (bool, optional): _description_. Defaults to False.

        Returns:
            _type_: _description_
        """

        # read entire file content and upload to blob
        if not stream:
            with open(file_path, "rb") as file:
                return self.upload(container=container, blob=blob, data=file.read())

        # read file content in chunks and upload each chunk
        if stream:
            blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)

            def read_large_file(file_path, chunk_size=4 * 1024 * 1024):
                """Generator function to read a large file in chunks."""

                with open(file_path, "rb") as file:
                    while True:
                        data = file.read(chunk_size)
                        if not data:
                            break
                        yield data

            blob_client.upload_blob(
                data=read_large_file(file_path=file_path),
            )

            return f"{container}/{blob}"

    def stream_to_local_file(
        self,
        file_path: str,
        container: str,
        blob: str,
    ):
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)

        stream = blob_client.download_blob()

        with open(file=file_path, mode="wb") as f:
            for chunk in stream.chunks():
                f.write(chunk)
