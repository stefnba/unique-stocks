from shared.types import DataLakeDataFileTypes, DataLakeZone

from utils.filesystem.data_lake.types import DataLakePathAzure, DataLakePathSerialized


class DataLakePathBase:
    """Factory for a path on Data Lake."""

    _zone: DataLakeZone
    _format: DataLakeDataFileTypes

    def __str__(self):
        return self.path

    @property
    def zone(self):
        return self._zone

    @zone.setter
    def zone(self, zone: DataLakeZone):
        self._zone = zone

    @property
    def container(self):
        return self._zone

    @property
    def directory(self):
        return None

    @property
    def path(self) -> str:
        return (self.directory or "") + (self.filename or "")

    @property
    def filename(self) -> str:
        return ""

    @property
    def format(self):
        return self._format

    @property
    def extension(self):
        return "." + self.format

    @property
    def serialized(self) -> DataLakePathSerialized:
        return {
            "format": self.format,
            "filename": self.filename,
            "directory": self.directory,
            "path": self.path,
            "container": self.container,
        }

    @property
    def azure_path(self) -> DataLakePathAzure:
        return {
            "blob_name": self.path,
            "container_name": self.container,
        }
