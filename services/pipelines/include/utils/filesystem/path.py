import uuid
from datetime import datetime
from pathlib import Path
import os
from shared.types import DataLakeDataFileTypes
from settings import config_settings
import shutil


class TempDirPath:
    """
    Builds a directory path that can be used to store temporary files in it.
    Directory can be deleted with .cleanup().
    """

    base_dir: str
    path: str

    def __str__(self) -> str:
        return self.path

    def __init__(self, base_dir: str) -> None:
        self.base_dir = base_dir

        self._build()

    def _build(self):
        path = Path(self.base_dir, random_name())
        # create dir if not exists
        path.mkdir(parents=True, exist_ok=True)
        self.path = f"{path.as_posix()}/"

    @classmethod
    def create(cls):
        tmp = cls(base_dir=config_settings.app.temp_dir_path)
        return tmp.path


class TempFilePath:
    """
    Builds a file path that can be used to store temporary files.
    File can be deleted with .cleanup().
    """

    path: str
    base_dir: str
    file_format: DataLakeDataFileTypes

    def __init__(self, base_dir: str, file_format: DataLakeDataFileTypes = "parquet") -> None:
        self.file_format = file_format
        self.base_dir = base_dir

        self._build()

    def __str__(self) -> str:
        return self.path

    def _build(self):
        if self.file_format:
            # create dir if not exists
            Path(self.base_dir).mkdir(parents=True, exist_ok=True)
            self.path = Path(self.base_dir, f"{random_name()}.{self.file_format.lstrip('.')}").as_posix()

    def cleanup(self):
        """Deletes file."""
        if Path(self.path).is_file():
            os.remove(self.path)

    @classmethod
    def create(cls, file_format: DataLakeDataFileTypes = "parquet"):
        tmp = cls(base_dir=config_settings.app.temp_dir_path, file_format=file_format)
        return tmp.path


def random_name():
    time = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    key = uuid.uuid4().hex

    return f"{time}_{key}"


def cleanup_tmp_dir():
    """Deletes all files within local temporary directory."""
    shutil.rmtree(config_settings.app.temp_dir_path)
