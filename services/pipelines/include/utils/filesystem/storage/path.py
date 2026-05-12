import typing as t
from datetime import datetime as dt
from typing import ClassVar
from urllib.parse import urlsplit
from uuid import uuid4

from utils.filesystem.storage.exceptions import ADLSUriParseFailure, S3UriParseFailure
from utils.filesystem.storage.types import FileTypes, S3Schemes, Schemes
from utils.filesystem.storage.utils import flatten_list, is_valid_adls_schema, is_valid_file_type, is_valid_s3_schema


class StoragePath:
    __version__: ClassVar[int] = 1

    _scheme: Schemes
    _dirs: t.Optional[list[str]] = []
    _root: t.Optional[str] = None
    _filename: t.Optional[str] = None
    _extension: t.Optional[FileTypes] = None

    def __init__(
        self,
        path: "str | StoragePath",
        root: t.Optional[str] = None,
        scheme: t.Optional[str] = None,
    ) -> None:
        if isinstance(path, str) and not root and not scheme:
            self._parse(path)
        elif isinstance(path, StoragePath):
            self._parse(path.uri)
        else:
            p = self.join_components(scheme, root, path)
            self._parse(p)

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        return f"<StoragePath uri='{self.uri}'>"

    def serialize(self):
        return str(self.uri)

    @staticmethod
    def deserialize(uri: str, version: int):
        return StoragePath(uri)

    def _parse(
        self,
        path: str,
    ) -> None:
        scheme: Schemes = "file"
        root = None

        if S3Path.is_s3_path(path):
            scheme, root, path = S3Path.parse_s3_uri(path)
        elif ADLSPath.is_adls_path(path):
            scheme, root, path = ADLSPath.parse_adls_uri(path)
        elif LocalPath.is_local_path(path):
            scheme, root, path = LocalPath.parse_local_path(path)

        self._scheme = scheme
        self._root = root
        self.path = path

    @classmethod
    def parse_string(cls, path_string: str) -> "StoragePath":
        return cls(path_string)

    @classmethod
    def parse_dict(cls, path_dict: dict) -> "StoragePath":
        raise NotImplementedError("Parsing from dict is not implemented yet.")

    @property
    def dirs(self) -> str:
        """Return parent directories as string joined by '/'."""

        if not self._dirs:
            return ""

        return self._join_path_elements(*self._dirs)

    @dirs.setter
    def dirs(self, dirs: str | list[str]):
        """
        Set parent directories from string or list of directory paths.
        """
        if isinstance(dirs, str):
            self._dirs = dirs.split("/")
        else:
            self._dirs = dirs

    def is_file(self) -> bool:
        """Check if path is a file."""
        return bool(self.extension)

    def is_dir(self) -> bool:
        """Check if path is a directory."""
        return not self.is_file()

    def is_cloud(self) -> bool:
        """Check if path is a cloud path."""
        return self.scheme != "file"

    @property
    def name(self) -> str | None:
        """Return filename including extension."""

        if self.is_file():
            return f"{self.stem}.{self.extension}"

        return None

    @property
    def stem(self) -> str | None:
        """Return filename without extension."""
        return self._filename

    @property
    def extension(self) -> str | None:
        """Return file extension."""
        return self._extension

    @extension.setter
    def extension(self, extension: FileTypes):
        """Set file extension."""
        self._extension = extension

    @property
    def root(self) -> str | None:
        """Return root directory."""
        return self._root

    @property
    def scheme(self) -> str:
        """Return scheme."""
        return self._scheme

    @property
    def path(self) -> str:
        """
        Return full path including directories, filename and extension but
        without root and scheme.
        """
        p = self._join_path_elements(self.dirs, self.name)

        if self.is_cloud():
            return p.lstrip("/")

        return p

    @path.setter
    def path(self, path: str | list[str]):
        """
        Set directories, filename and extension from path string or list of path elements.

        Args:
            path (str | list[str]): path string or list of path elements.
        """

        elements: list[str] = []

        if isinstance(path, str):
            elements = path.split("/")
        else:
            elements = path

        # check if path contains a file
        last_element = elements[-1]
        if "." in last_element:
            # set filename and extension
            file_elements = last_element.split(".")
            filename = file_elements[0]
            extension = ".".join(file_elements[1:])
            if not is_valid_file_type(extension):
                raise ValueError(f"File type {extension} is not supported.")

            self._filename = filename
            self._extension = extension

            # remove filename and extension from path elements
            elements = elements[:-1]

        self._dirs = elements

    @property
    def uri(self) -> str:
        """Return full path including root and scheme."""
        return self.scheme + "://" + self._join_path_elements(self.root, self.path)

    def to_dict(self):
        """Return path as dict."""
        return {
            "scheme": self.scheme,
            "root": self.root,
            "dirs": self.dirs,
            "filename": self.name,
            "extension": self.extension,
            "path": self.path,
            "uri": self.uri,
        }

    @staticmethod
    def _join_path_elements(*path_elements: t.Optional[str | list[str | None]]) -> str:
        """
        Combine path elements into a single path string.
        None elements are ignored and '/' for each path element are stripped, except for the first element to
        indicate an absolute path.
        """

        if not path_elements:
            return ""

        e = flatten_list(path_elements)

        def _join(item: tuple[int, str | None]):
            index, value = item
            if value is None:
                return
            if index == 0:
                return value.rstrip("/")
            return value.strip("/")

        return "/".join([x for x in list(map(_join, enumerate(e))) if x is not None])
        # return "/".join(e)

    @staticmethod
    def join_components(scheme: t.Optional[str], root: t.Optional[str], path: str) -> str:
        """Create a StoragePath from scheme, root and path."""

        p = path

        if root:
            p = StoragePath._join_path_elements(root, path)

        if scheme:
            p = scheme + "://" + p

        return p


class LocalPath(StoragePath):
    @staticmethod
    def is_local_path(path: str) -> bool:
        """Check if path is a local filesystem path."""

        if path.startswith("file://"):
            return True

        if "//" in path:
            return False

        return True

    @staticmethod
    def parse_local_path(path: str):
        """Parse local path into scheme, root and path."""

        if not LocalPath.is_local_path(path):
            raise ValueError(f"Failed to parse local path: {path}")

        if path.startswith("file://"):
            path = path.replace("file://", "")

        return "file", None, path


class ADLSPath(StoragePath):
    @staticmethod
    def is_adls_path(path: str) -> bool:
        """Check if path is an ADLS path."""
        return path.startswith("abfs://")

    @staticmethod
    def parse_adls_uri(adls_uri: str):
        """Parse ADLS URI into scheme, account name and path."""

        parsed_uri = urlsplit(adls_uri)

        if not parsed_uri.netloc:
            raise ADLSUriParseFailure(f"Failed to parse ADLS URI: {adls_uri}")

        if not is_valid_adls_schema(parsed_uri.scheme):
            raise ADLSUriParseFailure(f"Failed to parse ADLS URI: {adls_uri}")

        scheme = parsed_uri.scheme
        container = parsed_uri.netloc
        blob = parsed_uri.path.lstrip("/")

        return scheme, container, blob

    @property
    def container(self) -> str:
        """Return container name."""

        if not self.root:
            raise ValueError("ADLS path must contain a container.")
        return self.root

    @property
    def blob(self) -> str:
        """Return blob."""
        return self.path


class S3Path(StoragePath):
    @property
    def bucket(self) -> str:
        """Return bucket name."""

        if not self.root:
            raise ValueError("S3 path must contain a bucket.")
        return self.root

    @property
    def key(self) -> str:
        """Return key."""
        return self.path

    @staticmethod
    def is_s3_path(path: str) -> bool:
        """Check if path is an S3 path."""
        return path.startswith("s3://")

    @staticmethod
    def parse_s3_uri(s3_uri: str) -> t.Tuple[S3Schemes, str, str]:
        """Parse S3 URI into scheme, bucket and key."""

        parsed_uri = urlsplit(s3_uri)

        if not parsed_uri.netloc:
            raise S3UriParseFailure(f"Failed to parse S3 URI: {s3_uri}")

        if not is_valid_s3_schema(parsed_uri.scheme):
            raise S3UriParseFailure(f"Invalid S3 scheme: {parsed_uri.scheme}")

        scheme = parsed_uri.scheme
        bucket_name = parsed_uri.netloc
        key = parsed_uri.path.lstrip("/")

        return scheme, bucket_name, key


class UtilityStoragePath:
    scheme: Schemes = "file"
    root: t.Optional[str] = None
    path_prefix: t.Optional[str] = None

    _path: StoragePath
    path_factory: t.Type[StoragePath] = StoragePath

    @property
    def path(self):
        return self._path

    @property
    def uri(self):
        return self.path.uri

    def to_dict(self):
        return self.path.to_dict()


T = t.TypeVar("T", bound=StoragePath)


class TempStoragePath(UtilityStoragePath, t.Generic[T]):
    """Create a temporary path, either for a directory or a file."""

    _path: T

    def __init__(self) -> None:
        path = StoragePath._join_path_elements(
            self.path_prefix,
            uuid4().hex,
        )

        self._path = self.path_factory(path=path, root=self.root, scheme=self.scheme)  # type: ignore

    @property
    def path(self) -> T:
        return self._path

    @classmethod
    def create_file(cls, type: FileTypes = "parquet"):
        c = cls()

        current_path = c.path._dirs

        if current_path:
            name = current_path.pop()
            filename = name + "." + type

            c.path.path = StoragePath._join_path_elements(*current_path, filename)

        return c

    @classmethod
    def create_dir(cls):
        return cls()


class RawZoneStoragePath(UtilityStoragePath, t.Generic[T]):
    """Creae a path to a raw zone of a Cloud Data Lake."""

    _path: T

    def __init__(self, product, source, type: FileTypes, partition: t.Optional[t.Dict[str, str]] = None) -> None:
        self.now = dt.now()

        root = self.root
        path = StoragePath._join_path_elements(
            self.path_prefix,
            product,
            source,
            self.now.strftime("%Y/%m/%d/%H-%M-%S_%f"),
            *self.create_partition_dir_path_element(partition) if partition else [],
            uuid4().hex + "." + type,
        )

        self._path = self.path_factory(path=path, root=root, scheme=self.scheme)  # type: ignore

    @classmethod
    def parse(cls, path: str, extension: FileTypes) -> "RawZoneStoragePath[T]":
        c = cls("", "", extension)
        c._path = c.path_factory.parse_string(path_string=path)  # type: ignore
        c._path.extension = extension
        return c

    @property
    def path(self) -> T:
        return self._path

    def add_partition(self, partition: t.Dict[str, str]) -> T:
        """Add partition to path."""

        path = StoragePath._join_path_elements(
            self.path.dirs,
            *self.create_partition_dir_path_element(partition),
            (uuid4().hex + "." + self.path.extension) if self.path.extension else None,
        )

        return self.path_factory(path=path, root=self.root, scheme=self.scheme)  # type: ignore

    def create_partition_dir_path_element(self, dict: t.Dict[str, str]):
        """Create a path element for a partition based in Hive partitioning flavor."""
        return [f"{k}={v}" for k, v in dict.items()]
