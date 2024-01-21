import typing as t
from pathlib import Path as P

from settings import config_settings
from utils.filesystem.storage.path import ADLSPath, RawZoneStoragePath, S3Path, StoragePath, TempStoragePath

T = t.TypeVar("T", bound="StoragePath")


S3_BUCKET = "uniquestocks"
S3_ROOT = "data-lake"


class LocalStoragePath(TempStoragePath[StoragePath]):
    root = config_settings.app.temp_dir_path
    scheme = "file"

    @property
    def full_path(self):
        return self.path.path

    @classmethod
    def create_dir(cls):
        path = super().create_dir()

        # create dir if not exists
        P(path.path.path).mkdir(parents=True, exist_ok=True)

        return path

    @classmethod
    def create_file(cls, type="paruqet"):
        path = super().create_file(type=type)

        # create dir if not exists
        P(path.path.path).parent.mkdir(parents=True, exist_ok=True)

        return path


class S3TempPath(TempStoragePath[S3Path]):
    root = S3_BUCKET
    scheme = "s3"
    path_prefix = f"{S3_ROOT}/temp"

    path_factory = S3Path

    @property
    def bucket(self) -> str:
        return self.path.bucket

    @property
    def key(self) -> str:
        return self.path.key


class S3RawZonePath(RawZoneStoragePath[S3Path]):
    root = S3_BUCKET
    scheme = "s3"
    path_prefix = f"{S3_ROOT}/raw"

    path_factory = S3Path

    @property
    def bucket(self) -> str:
        return self.path.bucket

    @property
    def key(self) -> str:
        return self.path.key


class ADLSRawZonePath(RawZoneStoragePath[ADLSPath]):
    root = "raw"
    scheme = "abfs"

    path_factory = ADLSPath

    @property
    def container(self) -> str:
        return self.path.container

    @property
    def blob(self) -> str:
        return self.path.blob


class ADLSTempPath(TempStoragePath[ADLSPath]):
    root = "temp"
    scheme = "abfs"

    path_factory = ADLSPath

    @property
    def container(self) -> str:
        return self.path.container

    @property
    def blob(self) -> str:
        return self.path.blob
