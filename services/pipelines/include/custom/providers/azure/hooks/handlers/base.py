from adlfs import AzureBlobFileSystem
from shared.types import DataLakeDatasetFileTypes
from custom.providers.azure.hooks.types import Dataset
from utils.filesystem.path import Path, AdlsPath
from typing import Optional


class DatasetHandler:
    path: Path
    format: DataLakeDatasetFileTypes

    def __init__(
        self,
        path: Path,
        format: Optional[DataLakeDatasetFileTypes] = "parquet",
        **kwargs,
    ):
        self.path = path
        self.format = format or "parquet"

    def read(self, **kwargs) -> Dataset:
        raise NotImplementedError()

    def write(self, dataset: Dataset, **kwargs) -> str:
        raise NotImplementedError()


class AzureDatasetHandler(DatasetHandler):
    """
    Factory handler for a cloud dataset on Azure Data Lake Storage.
    Raises an exception if `container`, `filesystem` or `conn_id` is not specified."""

    path: AdlsPath
    filesystem: AzureBlobFileSystem
    conn_id: str

    def __init__(self, path: Path, format: DataLakeDatasetFileTypes = "parquet", **kwargs):
        super().__init__(path, format, **kwargs)

        self.init_azure_dataset(**kwargs)

    def init_azure_dataset(self, conn_id: str, filesystem: AzureBlobFileSystem):
        """"""

        self.path = AdlsPath(**Path.create(self.path).to_dict())

        self.conn_id = conn_id
        self.filesystem = filesystem

        if not self.filesystem:
            raise Exception("No filesystem")

        if not self.conn_id:
            raise Exception("not conn id")

    # def _check_paths_adlfs(self, path: str) -> list[str] | str:
    #     """
    #     Use adlfs wrapper to get files if glob pattern found, otherwise return provided path as is..
    #     """
    #     if "*" in path or "**" in path or "?" in path or "[..]" in path:
    #         return cast(list[str], self.filesystem.glob(path))

    #     return path

    # def _find_paths(self, path: str | list[str], container: str, prefix: Optional[str] = None, use_adlfs_for_glob=True):
    #     """
    #     Find all files for specified path and container.
    #     """

    #     if isinstance(path, list):
    #         all_paths: list[str] = []

    #         for p in path:
    #             if use_adlfs_for_glob:
    #                 matches = self._check_paths_adlfs(
    #                     AzureDatasetBaseHandler.concatenate_path_container(path=p, container=container, prefix=prefix)
    #                 )
    #                 if isinstance(matches, str):
    #                     all_paths.append(matches)
    #                 else:
    #                     print(len(matches))
    #                     all_paths.extend(matches)
    #             else:
    #                 all_paths.append(
    #                     AzureDatasetBaseHandler.concatenate_path_container(path=p, container=container, prefix=prefix)
    #                 )

    #         return all_paths

    #     # add container
    #     if use_adlfs_for_glob:
    #         return self._check_paths_adlfs(
    #             AzureDatasetBaseHandler.concatenate_path_container(path=path, container=container, prefix=prefix)
    #         )
    #     return AzureDatasetBaseHandler.concatenate_path_container(path=path, container=container, prefix=prefix)

    # @staticmethod
    # def concatenate_path_container(path: str, container: str, prefix: Optional[str] = None):
    #     """Joins container name and path of file within container."""

    #     joined = "/".join([container, path])

    #     if prefix and not joined.startswith(prefix):
    #         return prefix + joined

    #     return joined
