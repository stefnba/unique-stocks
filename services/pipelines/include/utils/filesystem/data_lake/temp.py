from typing import Sequence
from shared.types import DataLakeDataFileTypes
from utils.filesystem.path import random_name


from utils.filesystem.data_lake.base import DataLakePathBase


class TempDataLakePath(DataLakePathBase):
    """Factory for a temporary Data Lake path."""

    _zone = "temp"
    _format = "parquet"


class TempFile(TempDataLakePath):
    _filename: str

    def __init__(self, format: DataLakeDataFileTypes = "parquet") -> None:
        self._format = format
        self._filename = self.randon_iter

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def randon_iter(self):
        """Random filename each time property is accessed."""
        return random_name() + self.extension

    @property
    def directory(self):
        return None


class TempDirectory(TempDataLakePath):
    """Creates a path to a temporary directory that can be used as a bucket for files."""

    template_fields: Sequence[str] = ("base_dir",)

    base_dir: str

    def __init__(self, base_dir: str = random_name(), format: DataLakeDataFileTypes = "parquet"):
        self.base_dir = base_dir.rstrip("/") + "/"

    @property
    def directory(self):
        return self.base_dir

    @property
    def filename(self):
        return self.distinct_filename()

    def distinct_filename(self):
        return random_name() + self.extension

    def distinct_path(self):
        return self.directory + self.distinct_filename()
