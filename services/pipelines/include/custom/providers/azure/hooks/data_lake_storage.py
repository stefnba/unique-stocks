import asyncio
import logging
import typing as t
from dataclasses import dataclass
from pathlib import Path

import aiofiles
from aiohttp import ClientResponseError
from airflow.hooks.base import BaseHook
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.storage.blob.aio import ContainerClient as AsyncContainerClient
from custom.providers.azure.hooks.types import AzureDataLakeCredentials
from shared.types import DataLakeZone
from utils.file.stream import read_large_file
from utils.filesystem.directory import DirFile, scan_dir_files
from utils.filesystem.path import AdlsPath, LocalPath, PathInput
from utils.parallel.asyncio import AsyncIoRoutine, Counter, RateLimiter, make_async_iterable


class AzureCredentialBaseHook(BaseHook):
    """
    Abstract base class for ADLS hooks that retrieves connection details
    and assigns credential properties.
    """

    conn_id: str
    credentials: AzureDataLakeCredentials

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.credentials = self.get_conn()

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


@dataclass
class UrlUploadRecord:
    endpoint: str
    blob: t.Optional[str] = None
    param: t.Optional[dict] = None


class AzureDataLakeStorageHook(AzureCredentialBaseHook):
    """
    Integration with Azure Blob Storage.

    It communicate via the Window Azure Storage Blob protocol. Contrary to the Airflow Azure WASB hook, it can also
    utilize a `adls` connection.
    """

    blob_service_client: BlobServiceClient

    def __init__(self, conn_id: str) -> None:
        super().__init__(conn_id=conn_id)

        self.blob_service_client = BlobServiceClient(
            account_url=self.credentials.account_url,
            credential=ClientSecretCredential(
                client_id=self.credentials.client_id,
                client_secret=self.credentials.secret,
                tenant_id=self.credentials.tenant_id,
            ),
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
        blob_type: t.Literal["BlockBlob", "PageBlob", "AppendBlob"] = "BlockBlob",
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


class AzureDataLakeStorageBulkHook(AzureCredentialBaseHook):
    """
    Combines methods to interact with ADLS with many files in parallel. Useful for
    bulk upload and downloading files using the `asyncio` and `azure.storage.blob.aio` packages.
    """

    RecordUrlUpload = UrlUploadRecord

    def get_async_container_client(self, container: str):
        """
        Returns async ContainerClient.
        """
        return AsyncContainerClient(
            container_name=container,
            account_url=self.credentials.account_url,
            credential=ClientSecretCredential(
                client_id=self.credentials.client_id,
                client_secret=self.credentials.secret,
                tenant_id=self.credentials.tenant_id,
            ),  # type: ignore
        )

    def get_container_client(self, container: str):
        """
        Returns sync ContainerClient.
        """
        return ContainerClient(
            container_name=container,
            account_url=self.credentials.account_url,
            credential=ClientSecretCredential(
                client_id=self.credentials.client_id,
                client_secret=self.credentials.secret,
                tenant_id=self.credentials.tenant_id,
            ),  # type: ignore
        )

    async def _make_async_iterator(self, input_blobs: t.Iterable):
        """
        Turn a list of blob names or generator of blob names into an async generator that can be used to downloads
        multiple blobs at the same time.

        Args:
            input_blobs (t.Iterable):
                List or generator of blob names to download.

        Yields:
            blob: Name of blob, i.e. path on ADLS.
        """
        for b in input_blobs:
            yield b

    def download_blobs_from_list(
        self,
        container: str,
        blobs: list[str],
        destination_dir: str,
        destination_nested_relative_to: t.Optional[str] = None,
    ):
        """
        Download all specifed blobs into a local directory. Filename of downloaded blobs is the last part of the blob
        name. All blobs must reside in same container.

        """
        r = DownloadBlobsRoutine("DownloadBlobs")
        r.container_client = self.get_async_container_client(container=container)
        return r.run(
            blobs=blobs,
            destination_dir=destination_dir,
            destination_nested_relative_to=destination_nested_relative_to,
        )

    def download_blobs(
        self,
        container: str,
        blob_starts_with: str,
        destination_dir: str,
    ):
        """Download all blobs that start with a specific name into a local directory."""

        container_client = self.get_container_client(container=container)
        blobs = container_client.list_blob_names(name_starts_with=blob_starts_with)

        r = DownloadBlobsRoutine("DownloadBlobs")
        r.container_client = self.get_async_container_client(container=container)
        return r.run(blobs=blobs, destination_dir=destination_dir)

    def upload_from_url(
        self,
        url_endpoints: t.Iterable[str] | t.Iterable[UrlUploadRecord],
        container: str,
        blob_dir: t.Optional[str] = None,
        base_url: t.Optional[str] = None,
        base_params: t.Optional[dict] = None,
    ):
        """Directly transfer data from HTTP calls to ADLS blobs."""

        routine = UploadFromUrlRoutine(name="UploadFromUrl")
        routine.container_client = self.get_async_container_client(container=container)
        return routine.run(
            url_endpoints=url_endpoints,
            base_url=base_url,
            base_params=base_params,
            blob_dir=blob_dir,
        )

    def upload_directory(
        self,
        container: str,
        blob_dir: str,
        local_dir: str,
        include_sub_dirs=True,
        keep_sub_dir_structure=True,
    ):
        """
        Upload all files within a local directory in parallel.
        """

        files = scan_dir_files(dir=local_dir, include_sub_dirs=include_sub_dirs)
        r = UploadBlobsRoutine("UploadBlobs")
        r.container_client = self.get_async_container_client(container=container)
        return r.run(blob_dir=blob_dir, local_files=files)

    def upload_files(
        self,
        container: str,
        blob_dir: str,
        local_files: list[str],
    ):
        """
        Upload local files with the specified paths to a blob directory on ADLS.
        """

        r = UploadBlobsRoutine("UploadBlobs")
        r.container_client = self.get_async_container_client(container=container)
        return r.run(blob_dir=blob_dir, local_files=local_files)

    def delete_blobs(
        self, container: str, blobs: t.Optional[t.Iterable[str]] = None, start_with: t.Optional[str] = None
    ):
        r = DeleteBlobsRoutine("DeleteBlobs")
        r.container_client = self.get_async_container_client(container=container)

        if not blobs and start_with is not None:
            blobs = list(self.get_container_client(container).list_blob_names(name_starts_with=start_with))

        if blobs is None:
            self.log.info("No blobs were specified for deletion.")
            return

        if not blobs:
            self.log.info("No blobs were found.")
            return

        r.run(blobs=blobs)

        self.log.info(
            f"{len(blobs) if isinstance(blobs, list) else 'All'} blobs in container '{container}' were removed."
        )

        return blobs


class UploadFromUrlRoutine(AsyncIoRoutine):
    """
    Get data from API endpoint and directly upload content to a blob.
    """

    container_client: AsyncContainerClient

    async def coro_task(
        self,
        url: str,
        blob: str,
        params: t.Optional[dict] = {},
    ):
        result = None
        async with self.limiter, self.counter:
            try:
                async with self.http_client.get(url, params=params) as resp:
                    result = await resp.read()
            except ClientResponseError as err:
                logging.error(
                    f"{err.message} {url} {err.status}",
                )
                self.errors.append(err)

        if result:
            await self.container_client.upload_blob(name=blob, data=result, overwrite=True)

        return blob

    async def main(
        self,
        url_endpoints,
        blob_dir: t.Optional[str] = None,
        base_url: t.Optional[str] = None,
        base_params: t.Optional[dict] = None,
    ):
        self.http_client = AsyncIoRoutine.get_http_client()
        self.limiter = RateLimiter(rate_limit=2_000)
        self.counter = Counter(step=100)

        async with self.http_client, self.container_client:
            async for url in make_async_iterable(url_endpoints):
                blob = ""
                param = {}

                if isinstance(url, UrlUploadRecord):
                    if url.blob:
                        blob = url.blob
                    if url.param:
                        param = url.param
                    url = url.endpoint
                else:
                    raise ValueError("Blob name missing.")

                if blob_dir:
                    blob = str(Path(blob_dir) / blob)

                if base_url:
                    url = base_url.rstrip("/") + "/" + str(url).lstrip("/")

                self.coros.append(self.coro_task(url=url, blob=blob, params={**(base_params or {}), **param}))

            return await asyncio.gather(*self.coros)

    def run(
        self,
        url_endpoints: t.Iterable[str] | t.Iterable[UrlUploadRecord],
        blob_dir: t.Optional[str] = None,
        base_url: t.Optional[str] = None,
        base_params: t.Optional[dict] = None,
    ):
        return super().run(
            url_endpoints=url_endpoints,
            base_url=base_url,
            blob_dir=blob_dir,
            base_params=base_params,
        )


class DownloadBlobsRoutine(AsyncIoRoutine):
    container_client: AsyncContainerClient

    def __init__(self, name: str | None = None) -> None:
        super().__init__(name)

    async def coro_task(self, blob: str, local_file: str | Path):
        """Download blob content and save to local file."""

        data = await self.container_client.download_blob(blob=blob)

        async with aiofiles.open(local_file, mode="wb") as f:
            await f.write(await data.readall())

    async def main(
        self,
        blobs: t.Iterable,
        destination_dir: str,
        destination_nested_relative_to: t.Optional[str] = None,
    ):
        """Turn a list of blob names or generator of blob names into an async generator that can be used to downloads
        multiple blobs at the same time."""

        async with self.container_client:
            for blob in blobs:
                if not blob:
                    continue

                if destination_nested_relative_to:
                    local_file_path = Path(destination_dir) / Path(blob).relative_to(destination_nested_relative_to)
                    local_file_path.parent.mkdir(parents=True, exist_ok=True)
                else:
                    local_file_path = Path(destination_dir) / Path(blob).name

                self.coros.append(self.coro_task(blob=blob, local_file=local_file_path))

            return await asyncio.gather(*self.coros)

    def run(
        self,
        blobs: t.Iterable,
        destination_dir: str,
        destination_nested_relative_to: t.Optional[str] = None,
    ):
        return super().run(
            blobs=blobs,
            destination_dir=destination_dir,
            destination_nested_relative_to=destination_nested_relative_to,
        )


class UploadBlobsRoutine(AsyncIoRoutine):
    container_client: AsyncContainerClient

    async def coro_task(self, blob: str, local_file: str | Path):
        """Upload local files to blobs."""
        async with aiofiles.open(local_file, mode="rb") as f:
            data = await f.read()
        await self.container_client.upload_blob(name=blob, data=data)

    async def main(self, local_files: t.Iterable, blob_dir: str):
        """Turn a list of blob names or generator of blob names into an async generator that can be used to downloads
        multiple blobs at the same time."""
        async with self.container_client:
            for file in local_files:
                if not file:
                    continue

                blob = Path(blob_dir)
                local_file_path = ""

                if isinstance(file, DirFile):
                    local_file_path = file.path
                    blob = blob / file.rel_path

                if isinstance(file, str):
                    local_file_path = file
                    blob = blob / file

                self.coros.append(self.coro_task(local_file=local_file_path, blob=str(blob)))

            return await self.gather()

    def run(self, local_files: t.Iterable, blob_dir: str):
        return super().run(local_files=local_files, blob_dir=blob_dir)


class DeleteBlobsRoutine(AsyncIoRoutine):
    container_client: AsyncContainerClient

    async def main(self, blobs: t.Iterable):
        async with self.container_client:
            await self.container_client.delete_blobs(*blobs)

    def run(self, blobs: t.Iterable):
        return super().run(blobs=blobs)
