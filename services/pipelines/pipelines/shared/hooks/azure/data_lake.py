"""
Interact with Azure Data Lake Gen 2 through the DataLakeServiceClient.
Learn more on:
https://learn.microsoft.com/en-us/python/api/azure-storage-file-data_lake/azure.storage.filedatalake.datalakeserviceclient?view=azure-python
"""

from typing import TYPE_CHECKING, Optional

from azure.core.exceptions import ClientAuthenticationError, ResourceExistsError
from azure.core.paging import ItemPaged
from azure.storage.filedatalake import (
    DataLakeDirectoryClient,
    DataLakeFileClient,
    DataLakeServiceClient,
    FileSystemClient,
    PathProperties,
)
from shared.hooks.azure.base import AzureBaseClient
from shared.hooks.azure.types import DatalakeFile, DatalakeProperties, UploadMode
from shared.utils.path.builder import FilePathBuilder
from shared.utils.path.types import PathParams

if TYPE_CHECKING:
    from shared.utils.path.data_lake.file_path import DataLakeFilePathModel


import requests
from shared.loggers.logger import datalake as logger


import time


class AzureDatalakeHook(AzureBaseClient):
    """
    DatalakeServiceClient has several sub-clients:
    - FileSystemClient (similar to containers)
    - DatalakeDirectoryClient (similar to folders)
    - DataLakeFileClient
    """

    # clients
    service_client: DataLakeServiceClient
    file_system_client: FileSystemClient
    file_client: DataLakeFileClient

    def init(self):
        """
        Initiates DataLakeServiceClient.
        """
        try:
            credential = self.auth()
            self.service_client = DataLakeServiceClient(account_url=self.account_url, credential=credential)
        except ClientAuthenticationError as exception:
            print("Auth failed.", exception)
            raise

        except Exception as exception:
            print(exception)
            raise

    def create_file_system(self, name: str) -> FileSystemClient:
        """
        Create file systems/containers and return file system client
        """
        try:
            file_system_client = self.service_client.create_file_system(file_system=name)
            self.file_system_client = file_system_client
            return file_system_client

        except ResourceExistsError as exception:
            print(exception)
            raise exception

    def get_file_system(self, name: str) -> FileSystemClient:
        """
        Get a client to interact with the specified file system/container.
        """
        file_system_client = self.service_client.get_file_system_client(file_system=name)
        self.file_system_client = file_system_client
        return file_system_client

    def list_file_systems(self) -> list:
        """
        List file systems/containers
        """
        try:
            file_systems = self.service_client.list_file_systems()
            return list(file_systems)
        except Exception as exception:
            print(exception)
            raise exception

    def create_directory(
        self, name: str, file_system_client: FileSystemClient | None = None
    ) -> DataLakeDirectoryClient:
        """
        Create a new directory for provided FileSystemClient. If no
        """
        try:
            if file_system_client is None:
                file_system_client = self.file_system_client
            if file_system_client is None:
                raise Exception("Directory cannot be created, no FileSystemClient provided.")
            directory_client = file_system_client.create_directory(name)

            return directory_client

        except Exception as exception:
            print(exception)
            raise exception

    def list_directory_contents(self, file_system: str, directory: str | None = None) -> ItemPaged[PathProperties]:
        """
        List directory contents by calling the FileSystemClient.get_paths method.
        """
        try:
            file_system_client = self.service_client.get_file_system_client(file_system=file_system)
            paths: ItemPaged[PathProperties] = file_system_client.get_paths(path=directory)

            for path in paths:
                print(path.name)

            return paths

        except Exception as exception:
            print(exception)
            raise exception

    def get_file_client(self, path: "str | DataLakeFilePathModel") -> DataLakeFileClient:
        """
        Create a new File Client
        """
        _path = FilePathBuilder.convert_to_file_path(path)
        file_client = self.service_client.get_file_client(self.file_client, _path)
        self.file_client = file_client
        return file_client

    def stream_from_url(
        self, url: str, file_name: Optional["str | DataLakeFilePathModel"] = None, *, chunk_size: int = 10
    ):
        """
        Transfer a file from a url to the Data Lake in chunks.

        Args:
            url (str): _description_
            file_system_client (FileSystemClient): _description_
            file_name (str): _description_
            chunk_size (int, optional): _description_. Defaults to 20.
        """
        if file_name is None:
            file_name = url.split("/")[-1]
        else:
            file_name = FilePathBuilder.convert_to_file_path(file_name)

        logger.info("Start streaming", url=url)

        chunk_size = chunk_size * 1024 * 1024  # to MB
        start_time = time.time()

        file_client = self.service_client.get_file_client(file_system=self.file_system, file_path=file_name)
        file_client.create_file()

        # make request
        with requests.get(url, stream=True) as r:
            if not r.ok:
                # todo log
                r.raise_for_status()

            total_size = 0
            uploaded_size = 0
            buffer_size = 0
            buffer = b""

            for chunk in r.iter_content(chunk_size=chunk_size):
                current_chunk_size = len(chunk)

                # add current chunk to buffer
                buffer += chunk
                buffer_size += current_chunk_size

                if buffer_size > chunk_size:
                    logger.info(f"Uploaded: {round(uploaded_size / 1024 / 1024, 2)} MB")

                    file_client.append_data(buffer, offset=uploaded_size, length=buffer_size)

                    uploaded_size += buffer_size
                    buffer = b""
                    buffer_size = 0

                total_size = total_size + current_chunk_size

            # uploading remaining data
            if buffer_size > 0:
                file_client.append_data(buffer, offset=uploaded_size, length=buffer_size)

            # commit
            file_client.flush_data(offset=total_size)

        logger.info(f"Time: {time.time() - start_time} | Total Size: {round(total_size / 1024 / 1024, 2)} MB")

        return str(file_client.get_file_properties().name)

    def download_file_into_memory(self, file_path: str, file_system: Optional[str] = None) -> bytes:
        """
        Download a file from the DataLake into memory.

        Args:
            file_path (str): Path to file in container. Should be an absolute path.
            file_system (str): Name of container.
        """
        _file_system = file_system or self.file_system

        if not _file_system:
            raise Exception("No file system specified.")

        try:
            file_client = self.service_client.get_file_client(file_system=_file_system, file_path=file_path)
            downloadde_file = file_client.download_file()

            return downloadde_file.readall()

        except Exception as exception:
            print(exception)
            raise exception

    def download_file(self, file_path: str, local_file_path: str, file_system: Optional[str] = None):
        """
        Download and save a file from the DataLake to local file system.

        Args:
            file_path (str): Path to file in container. Should be an absolute path.
            file_system (str): Name of container.
            local_file_path (str): Path on local system where file should be downloaded to.
        """
        _file_system = file_system or self.file_system

        if not _file_system:
            raise Exception("No file system specified.")

        try:
            file_client = self.service_client.get_file_client(file_system=_file_system, file_path=file_path)
            downloadde_file = file_client.download_file()
            downloaded_bytes = downloadde_file.readall()

            with open(local_file_path, "wb") as local:
                local.write(downloaded_bytes)

        except Exception as exception:
            print(exception)
            raise exception

    def delete_file(
        self,
        file_path: PathParams | "DataLakeFilePathModel",
        file_system: Optional[str] = None,
    ):
        """
        Delete a file from the DataLake.

        Args:
            file_path: Path of file to be deleted.
            file_system: File system, i.e. container. Defaults to None.
        """
        _file_system = file_system or self.file_system
        _file_path = FilePathBuilder.convert_to_file_path(file_path)

        if not _file_system:
            raise Exception("No file system specified.")

        try:
            file_client = self.service_client.get_file_client(file_system=_file_system, file_path=_file_path)
            file_client.delete_file()
        except Exception as exception:
            print("File could not be deleted.")
            raise exception

    def upload_file(
        self,
        file: str | bytes,
        destination_file_path: PathParams | "DataLakeFilePathModel",
        file_system: Optional[str] = None,
        mode: UploadMode = "upload",
    ) -> DatalakeFile:
        """
        Uploads a file - either from bytes or local file system - into DataLake.

        Args:
            file (str | bytes): File to upload.
            destination_file_path (PathArgs | Type[DataLakeFilePathModel] | DataLakeFilePathModel): Path on DataLake where file should be
                uploaded to.
            file_system (Optional[str]): Container. If not provided, will look for self.file_system.


        Returns:
            DatalakeFile: Uploaded file with DatalakeFile properties.
        """
        _file_system = file_system or self.file_system
        _destination_file_path = FilePathBuilder.convert_to_file_path(destination_file_path)

        if not _file_system:
            raise Exception("No file system specified.")

        try:
            file_client = self.service_client.get_file_client(
                file_system=_file_system, file_path=_destination_file_path
            )
            file_client.create_file()

            # populate content
            file_contents = None
            if isinstance(file, str):
                with open(file, "rb") as _local_file:
                    file_contents = _local_file.read()
            if isinstance(file, bytes):
                file_contents = file

            if file_contents is None:
                raise ValueError("No file content provided.")

            if mode == "append":
                file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
                file_client.flush_data(len(file_contents))

            else:
                file_client.upload_data(file_contents, overwrite=True)

            uploaded_file_path = file_client.path_name

            # make uploaded file path absolute
            if not uploaded_file_path.startswith("/"):
                uploaded_file_path = f"/{uploaded_file_path}"

            uploaded_file = FilePathBuilder.parse_file_path(uploaded_file_path)

            return DatalakeFile(
                file=uploaded_file,
                data_lake=DatalakeProperties(
                    file_system=file_client.file_system_name,
                    storage_account=str(file_client.account_name),
                    storage_account_url=self.account_url,
                ),
            )

        except Exception as exception:
            print("File could not be uploaded.")
            raise exception
