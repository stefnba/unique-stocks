from adlfs import AzureBlobFileSystem
from typing import Optional, cast
from shared.types import DataLakeDataFileTypes
from custom.providers.azure.hooks.errors import ContainerNotSpecifieError
from custom.providers.azure.hooks.types import (
    Dataset,
)


class DatasetBaseHandler:
    path: str | list[str]
    format: DataLakeDataFileTypes

    def __init__(
        self,
        path: str | list[str],
        format: DataLakeDataFileTypes = "parquet",
        **kwargs,
    ):
        self.path = path
        self.format = format


class AzureDatasetBaseHandler:
    """
    Factory handler for a cloud dataset on Azure.
    Raises an exception if `container`, `filesystem` or `conn_id` is not specified."""

    container: str
    filesystem: AzureBlobFileSystem
    conn_id: str

    def init_azure_dataset(self, container: str, conn_id: str, filesystem: AzureBlobFileSystem):
        self.container = container
        self.conn_id = conn_id
        self.filesystem = filesystem

        self.check_azure_properties()

    def check_azure_properties(self) -> None:
        if not self.container:
            raise ContainerNotSpecifieError()

        if not self.filesystem:
            raise Exception("No filesystem")

        if not self.conn_id:
            raise Exception("not conn id")

    def _check_paths_adlfs(self, path: str) -> list[str] | str:
        """
        Use adlfs wrapper to get files if glob pattern found, otherwise return provided path as is..
        """
        if "*" in path or "**" in path or "?" in path or "[..]" in path:
            return cast(list[str], self.filesystem.glob(path))

        return path

    def _find_paths(self, path: str | list[str], container: str, prefix: Optional[str] = None, use_adlfs_for_glob=True):
        """
        Find all files for specified path and container.
        """

        if isinstance(path, list):
            all_paths: list[str] = []

            for p in path:
                if use_adlfs_for_glob:
                    matches = self._check_paths_adlfs(
                        AzureDatasetBaseHandler.concatenate_path_container(path=p, container=container, prefix=prefix)
                    )
                    if isinstance(matches, str):
                        all_paths.append(matches)
                    else:
                        print(len(matches))
                        all_paths.extend(matches)
                else:
                    all_paths.append(
                        AzureDatasetBaseHandler.concatenate_path_container(path=p, container=container, prefix=prefix)
                    )

            return all_paths

        # add container
        if use_adlfs_for_glob:
            return self._check_paths_adlfs(
                AzureDatasetBaseHandler.concatenate_path_container(path=path, container=container, prefix=prefix)
            )
        return AzureDatasetBaseHandler.concatenate_path_container(path=path, container=container, prefix=prefix)

    @staticmethod
    def concatenate_path_container(path: str, container: str, prefix: Optional[str] = None):
        """Joins container name and path of file within container."""

        joined = "/".join([container, path])

        if prefix and not joined.startswith(prefix):
            return prefix + joined

        return joined


class DatasetReadBaseHandler(DatasetBaseHandler):
    """Factory handler for reading a dataset."""

    def __init__(
        self,
        path: str | list[str],
        format: DataLakeDataFileTypes = "parquet",
        **kwargs,
    ):
        super().__init__(
            path,
            format,
            **kwargs,
        )

    def read(
        self,
        **kwargs,
    ) -> Dataset:
        raise NotImplementedError()


class DatasetWriteBaseHandler(DatasetBaseHandler):
    """Factory handler for writing a dataset."""

    dataset: Dataset
    path: str

    def __init__(
        self,
        dataset: Dataset,
        path: str,
        format: DataLakeDataFileTypes = "parquet",
        **kwargs,
    ):
        self.dataset = dataset
        super().__init__(
            path,
            format,
            **kwargs,
        )

    def write(
        self,
        **kwargs,
    ) -> str:
        raise NotImplementedError()


class AzureDatasetWriteBaseHandler(DatasetWriteBaseHandler, AzureDatasetBaseHandler):
    def __init__(
        self,
        dataset,
        path: str,
        container: str,
        filesystem: AzureBlobFileSystem,
        conn_id: str,
        format: DataLakeDataFileTypes = "parquet",
        **kwargs,
    ):
        super().__init__(
            dataset=dataset,
            path=path,
            format=format,
            **kwargs,
        )

        self.init_azure_dataset(container=container, conn_id=conn_id, filesystem=filesystem)


class AzureDatasetReadBaseHandler(DatasetReadBaseHandler, AzureDatasetBaseHandler):
    def __init__(
        self,
        path: str,
        container: str,
        filesystem: AzureBlobFileSystem,
        conn_id: str,
        format: DataLakeDataFileTypes = "parquet",
        **kwargs,
    ):
        super().__init__(
            path=path,
            format=format,
            **kwargs,
        )

        self.init_azure_dataset(container=container, conn_id=conn_id, filesystem=filesystem)
