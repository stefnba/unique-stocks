import typing as t
import uuid
from datetime import datetime as dt
from pathlib import Path as P
from urllib.parse import urlparse

Zones = t.Literal["raw", "temp"]
Providers = t.Literal["local", "s3", "adls"]
FileTypes = t.Literal["parquet", "csv", "json"]
Sources = t.Literal["Eod"]
Protocols = t.Literal["file", "s3", "s3a", "s3n", "abfs", ""]


def is_valid_protocol(protocol: str) -> t.TypeGuard[Protocols]:
    """Check if the protocol is valid."""

    if protocol in t.get_args(Protocols):
        return True
    return False


def is_valid_file_type(type: str) -> t.TypeGuard[FileTypes]:
    """Check if the protocol is valid."""

    if type not in t.get_args(FileTypes):
        raise ValueError(f"File type {type} is not supported.")
    return True


class StoragePathConfig(t.TypedDict):
    extract_root: bool  # whether to extract root from path string for cloud paths


storage_path_config_default: StoragePathConfig = {
    "extract_root": True,
}


class StoragePath:
    """
    Base Class to build and parse paths for local and cloud storage.
    """

    _protocol: Protocols
    _root: t.Optional[str] = None  # container for adls or bucket for s3
    _base_path: t.Optional[str] = None
    _path: str
    _file: t.Optional[str] = None
    _extension: t.Optional[str] = None

    _dir: str

    config: StoragePathConfig

    @t.overload
    def __init__(self, path: "str | StoragePath") -> None:
        ...

    @t.overload
    def __init__(
        self,
        path: str,
        protocol: t.Optional[Protocols] = None,
        root: t.Optional[str] = None,
        config: StoragePathConfig = storage_path_config_default,
    ) -> None:
        ...

    def __init__(
        self,
        path: "str | StoragePath",
        protocol: t.Optional[Protocols] = None,
        root: t.Optional[str] = None,
        config: StoragePathConfig = storage_path_config_default,
    ) -> None:
        storage_path_config_default.update(config)
        self.config = storage_path_config_default

        # only path as string specified
        if isinstance(path, str) and not root and not protocol:
            self._parse(path)
        # class instance specified
        elif isinstance(path, StoragePath):
            self._parse(path.uri)
        # if path, protocol and root specified, join them and still use parser
        else:
            self._parse(self.join_path_elements(protocol=protocol or self.protocol, root=root, path=path))

    def join_path_elements(
        self, protocol: t.Optional[Protocols] = None, root: t.Optional[str] = None, *, path: str
    ) -> str:
        """Join protocol, root and path into a path."""

        if protocol is not None and "://" in path:
            raise ValueError("Path is not allowed to contain a protocol when a protocol is specified.")

        p = ""

        if protocol is not None:
            p = protocol + "://"

        if root is not None:
            return self.join_single_paths(p + root, path)

        return p + path

    def serialize(self) -> dict[str, str | None]:
        return self.to_dict()

    @staticmethod
    def join_single_paths(*path: t.Optional[str]) -> str:
        """Join various paths into a single path."""

        return "/".join([(p.lstrip("/") if i > 0 else p).rstrip("/") for i, p in enumerate(path) if p is not None])

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.build_full_path()}>"

    def __str__(self) -> str:
        return self.build_full_path()

    def is_cloud(self) -> bool:
        """
        Check if the path is a cloud path in uri format.
        """
        return not self.is_local()

    def is_file(self) -> bool:
        """
        Check if the path is a file path.
        """
        return self.extension is not None

    def is_dir(self) -> bool:
        """
        Check if the path is a directory path.
        """
        return not self.is_file()

    def is_local(self) -> bool:
        """
        Check if the path is a local path.
        Local path is a path without protocol or with `file://` as protocol.
        """
        return self.protocol is None or self.protocol == "file"

    @classmethod
    def create_temp_file(cls, provider: Providers, type: FileTypes = "parquet"):
        """Create a temporary file for a given provider."""
        protocol = cls.set_protocol_from_provider(provider)
        root = "uniquestocks/data-lake"
        file = cls.create_random_file_name(type)

        return cls(protocol=protocol, root=root, path=file)

    @classmethod
    def create_temp_dir(cls, provider: Providers):
        """Create a temporary directory for a given provider."""
        protocol = cls.set_protocol_from_provider(provider)
        path = "random"

        return cls(protocol=protocol, root="uniquestocks/data-lake", path=path)

    @classmethod
    def create_raw(
        cls,
        source: str,
        product: str,
        type: FileTypes,
        partitioning={},
        provider: Providers = "local",
    ):
        """
        Build a path for raw zone in Data Lake.
        `raw` as a path element won't be added here because in different providers, we can use raw either a container or
        folder in a bucket.
        """

        protocol = cls.set_protocol_from_provider(provider)
        root = cls._root
        path = P(product, source)

        # add date
        path = path / dt.now().strftime("%Y/%m/%d/%H%M%S")

        if partitioning:
            partitioning_path = [f"{key}={value}" for key, value in partitioning.items() if value is not None]
            path = path / P(*partitioning_path)

        path = path / cls.create_random_file_name(type)

        return cls(protocol=protocol, path=path.as_posix(), root=root, config={"extract_root": True})

    @staticmethod
    def create_random_file_name(type: FileTypes):
        """Create a random file name using uuid4."""

        return uuid.uuid4().hex + "." + type

    def _parse(self, path: str) -> None:
        """
        Parse a string path into its components.
        """

        parsed = urlparse(path)

        # protocol
        if is_valid_protocol(parsed.scheme):
            self.protocol = parsed.scheme

        # extract root from path string
        if self.is_cloud() and self.config["extract_root"]:
            self.root = parsed.netloc
            self.path = parsed.path
            return None

        # make root = None and add hostname back to path
        self.root = None
        self.path = self.join_single_paths(parsed.netloc, parsed.path)

    @classmethod
    def parse(cls, path: str):
        """Classmethod to explicitly parse a string path into its components."""
        return cls(path)

    def build_full_path(self) -> str:
        """Construct absolute path for local path or uri for cloud path."""

        path = self.join_single_paths(self.base_path, self.path)

        # if self.is_cloud():
        #     return urlunparse([self.protocol, self.root or "", path, "", "", ""])

        return self.join_single_paths(self.root, path)

    def to_dict(self) -> dict[str, str | None]:
        """Return a dictionary representation of the path."""

        return {
            "protocol": self.protocol,
            "root": self.root,
            "path": self.join_single_paths(self.base_path, self.path),
            "dir": self.join_single_paths(self.base_path, self.dir),
            "file": self.file,
            "extension": self.extension,
        }

    @property
    def protocol(self):
        return self._protocol

    @protocol.setter
    def protocol(self, protocol: Protocols):
        """Set the protocol."""

        if protocol not in t.get_args(Protocols):
            raise ValueError(f"Protocol {protocol} is not supported.")

        if protocol == "":
            self._protocol = "file"
        else:
            self._protocol = protocol

    @staticmethod
    def set_protocol_from_provider(provider: Providers) -> Protocols:
        """Set protocol based on provider."""

        if provider == "s3":
            return "s3"

        if provider == "adls":
            return "abfs"

        if provider == "local":
            return "file"

    @property
    def full_path(self):
        """Property alias for build_full_path() method."""
        return self.build_full_path()

    @property
    def path(self):
        """Return path without filename and extension."""

        return self._path

    @path.setter
    def path(self, path: str):
        """
        Set the path. If the path has a filename or extension, set those as well.
        """

        p = P(path)

        # set extension
        if p.suffix:
            self.extension = t.cast(FileTypes, p.suffix)

            self.file = p.stem
            self._dir = p.parent.as_posix()
        else:
            self._dir = path

        self._path = path

    @property
    def file(self):
        return self._file

    @file.setter
    def file(self, filename: str):
        self._file = filename

    @property
    def dir(self):
        return self._dir

    @property
    def extension(self):
        return self._extension

    @extension.setter
    def extension(self, extension: FileTypes):
        """Set the extension."""

        # remove dot from extension
        if "." in extension:
            replaced = extension.replace(".", "")
            if is_valid_file_type(replaced):
                extension = replaced

        if extension not in t.get_args(FileTypes):
            raise ValueError(f"Extension {extension} is not supported.")

        self._extension = extension

    @property
    def root(self):
        return self._root

    @root.setter
    def root(self, value: t.Optional[str]) -> None:
        self._root = value

    @property
    def base_path(self):
        return self._base_path

    @base_path.setter
    def base_path(self, value: str) -> None:
        self._base_path = value

    @property
    def uri(self):
        """
        Return the full path as a uri.
        Local paths are returned as file://<path>.
        """

        if self.protocol == "file":
            return "file://" + self.build_full_path()

        return f"{self.protocol}://" + self.build_full_path()


class AdlsPath(StoragePath):
    _protocol: Protocols = "abfs"

    TEMP_CONTAINER: str = "temp"
    RAW_CONTAINER: str = "raw"

    @property
    def container(self):
        return self.root

    @container.setter
    def container(self, container: str):
        self.root = container

    @property
    def blob(self):
        return self.path

    @blob.setter
    def blob(self, blob: str):
        self.path = blob

    @classmethod
    def create_raw(cls, source: str, product: str, type: FileTypes, partitioning={}, provider: Providers = "adls"):
        return super().create_raw(source, product, type, partitioning, provider)


class S3Path(StoragePath):
    _protocol: Protocols = "abfs"

    @property
    def bucket(self):
        return self.root

    @bucket.setter
    def bucket(self, container: str):
        self.root = container

    @property
    def key(self):
        return self.path

    @key.setter
    def key(self, blob: str):
        self.path = blob

    @classmethod
    def create_raw(cls, source: str, product: str, type: FileTypes, partitioning={}, provider: Providers = "s3"):
        c = super().create_raw(source, product, type, partitioning, provider)

        base = c.join_single_paths(c.base_path or "", "raw")

        c.base_path = base
        return c

    @classmethod
    def create_temp_dir(cls):
        return super().create_temp_dir(provider="s3")
