import shutil
from settings import config_settings
import json

from typing import Any, Optional, get_args, cast, TypedDict, Dict, TypeAlias
from pydantic import BaseModel, Field, ConfigDict
from pathlib import Path as PathlibPath
from shared.types import DataLakeZone, DataLakeDatasetFileTypes, DataProducts, DataSources, DataLakeDataFileTypes
import uuid
from datetime import datetime

from utils.dag.xcom import XComGetter


class AdlsDict(TypedDict):
    blob: str
    container: DataLakeZone


class Path(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    root: str = ""
    path: str  # can be either directory or file
    format: Optional[str] = None
    dataset_format: Optional[DataLakeDatasetFileTypes] = Field(repr=False, default=None)
    protocol: str | None = Field(default=None, repr=False)

    _protocol: Optional[str] = None
    _temp_dir: str = ""

    def dump(self, **kwargs):
        """Convert FilePath model to python dict."""
        return self.model_dump(**kwargs)

    def to_dict(self, **kwargs) -> dict[str, Any]:
        """Convert Path model to python dict."""
        return self.dump(**kwargs)  # type: ignore

    def to_json(self, **kwargs) -> str:
        """Convert Path model to json."""
        return self.model_dump_json(**kwargs)

    def to_pathlib(self) -> PathlibPath:
        """Convert Path model to `pathlib.Path`."""
        return PathlibPath(self.root, self.path)

    def model_post_init(self, __context: Any) -> None:
        # check if path points to a file
        if PathlibPath(self.path).suffix:
            # path points to a file
            self.format = PathlibPath(self.path).suffix.replace(".", "")

        # check if format is a valid dataset format, otherwise dataset_format = None
        if self.format in get_args(DataLakeDatasetFileTypes):
            self.dataset_format = cast(DataLakeDatasetFileTypes, self.format)

    def set_protocol(self, protocol: str):
        """Allow setting of protocol after init."""
        self.protocol = protocol

    @classmethod
    def create_temp_dir_path(cls):
        return cls(root=cls._temp_dir.get_default(), path=uuid.uuid4().hex)  # type: ignore

    @classmethod
    def create_temp_file_path(cls, format: DataLakeDataFileTypes = "parquet"):
        return cls(root=cls._temp_dir.get_default(), path=uuid.uuid4().hex + f".{format}")  # type: ignore

    @classmethod
    def create(cls, path: "PathInput") -> "Path":
        """Takes `path` arg of various types and creates new `Path` model out of it."""

        if isinstance(path, dict):
            # duplicate pair path/root or container/blob into other pair
            if not ("container" in path and "blob" in path):
                path["container"] = path.get("root")
                path["blob"] = path.get("path")
            if not ("path" in path and "root" in path):
                path["root"] = path.get("container")
                path["path"] = path.get("blob")

            return cls(**path)
        if isinstance(path, str):
            # string can also be json
            try:
                j = json.loads(path)
                return cls.create(j)
            except ValueError:
                pass

            # real string
            p = PathlibPath(path)
            return cls(root=cast(DataLakeZone, p.parts[0]), path=p.relative_to(p.parts[0]).as_posix())
        if isinstance(path, Path):
            return path
        if isinstance(path, XComGetter):
            xcom_value = path.pull()
            if xcom_value:
                return cls.create(path=xcom_value)
        if isinstance(path, PathlibPath):
            return cls.create(path.as_posix())

        raise Exception(f"Parsing of path '{path}' failed.")

    @property
    def afls_path(self) -> AdlsDict:
        """Dump a python dict with `container` and `blob` keys."""
        afls = AdlsPath(**self.dump())
        return {
            "blob": afls.path,
            "container": afls.root,
        }

    @property
    def uri(self) -> str:
        """Build file uri with protocol (if specified), root and path."""

        full_path = ((PathlibPath(self.root) or PathlibPath("")) / self.path).as_posix()

        if self.protocol is not None:
            full_path = self.protocol.replace("://", "") + "://" + full_path.lstrip("/")

        return full_path

    @property
    def name(self) -> str:
        """The final path component (if any), i.e. the filename with extension."""
        return PathlibPath(self.path).name

    @property
    def stem(self) -> str:
        """The final path component w/o file extension."""
        return PathlibPath(self.path).stem

    @property
    def parts(self) -> tuple[str, ...]:
        return self.to_pathlib().parts


class AdlsPath(Path):
    """
    FilePath with required container that can be used for Azure Data Lake Storage.
    A valid blob can either be a directory or a file.
    """

    root: DataLakeZone = Field(alias="container")
    path: str = Field(alias="blob")

    _temp_dir: DataLakeZone = "temp"

    def model_post_init(self, __context: Any) -> None:
        return super().model_post_init(__context)

    @classmethod
    def create(cls, path: "PathInput") -> "AdlsPath":
        p = super().create(path)
        return cls(**p.to_dict())

    def to_dict(self, **kwargs) -> dict[str, Any]:
        return super().to_dict(by_alias=True, **kwargs)


class LocalPath(Path):
    _temp_dir: str = config_settings.app.temp_dir_path

    def model_post_init(self, __context: Any) -> None:
        return super().model_post_init(__context)

    def create_dir(self, parents=True, exist_ok=True) -> None:
        """
        Create a directory for concatenate path of `base_dir` and `path`.
        File `self.path` is a file, take parent of it, otherwise same level.
        """
        pathlib_path = PathlibPath(self.path)
        path = PathlibPath(self.root or "") / pathlib_path
        path.mkdir(parents=parents, exist_ok=exist_ok)

    @classmethod
    def create(cls, path: "PathInput") -> "LocalPath":
        p = super().create(path)
        return cls(**p.to_dict())

    @classmethod
    def create_temp_dir_path(cls):
        p = super().create_temp_dir_path()
        p.create_dir()
        return p


def cleanup_tmp_dir():
    """Deletes all files within local temporary directory."""
    shutil.rmtree(config_settings.app.temp_dir_path)


class PathElement:
    name: str
    hive_flavor: bool

    def __init__(self, name: str, hive_flavor=False) -> None:
        self.name = name
        self.hive_flavor = hive_flavor

    def path_element(self, value: str) -> str:
        if self.hive_flavor:
            return f"{self.name}={value}"
        return value


class AdlsDatasetPath:
    """
    Builds a path for dataset products in various zones or containers of the Data Lake in Azure.
    """

    product: DataProducts
    source: DataSources

    dir_template: list[str | PathElement] = [
        "product",
        "source",
        PathElement(name="year", hive_flavor=True),
        PathElement(name="month", hive_flavor=True),
        PathElement(name="day", hive_flavor=True),
    ]
    dir_path: PathlibPath

    filename_template: list[str | PathElement] | None = [
        "datetime",
        "product",
        "source",
    ]
    filename_sep = "__"
    filename: str | None = None

    args: dict

    def __init__(self, **kwargs) -> None:
        current = datetime.now()

        self.args = {
            **kwargs,
            "product": self.product,
            "year": current.strftime("%Y"),
            "month": current.strftime("%m"),
            "day": current.strftime("%d"),
            "date": current.strftime("%Y%m%d"),
            "time": current.strftime("%H%M%S"),
            "datetime": current.strftime("%Y%m%d-%H%M%S"),
        }

    @property
    def uri(self):
        self.build_dir()
        self.build_filename()
        return PathlibPath(self.dir_path, self.filename or "").as_posix()

    def build_path_element(self, element: str | PathElement):
        if isinstance(element, str):
            element = PathElement(element)

        value = self.args.get(element.name)

        if not value:
            return ""

        return element.path_element(value=value)

    def build_dir(self):
        self.dir_path = PathlibPath(*list(map(self.build_path_element, self.dir_template)))

    def build_filename(self):
        if not self.filename_template:
            return
        self.filename = (
            self.filename_sep.join([e for e in list(map(self.build_path_element, self.filename_template)) if e])
            + "."
            + self.args.get("format", "parquet")
        )

    @classmethod
    def raw(cls, format: DataLakeDataFileTypes, source: DataSources, **kwargs):
        c = cls(format=format, source=source, **kwargs)
        return AdlsPath(container="raw", blob=c.uri, format=format)

    @classmethod
    def transformed(cls):
        c = cls()
        c.dir_template = ["product"]
        c.filename_template = None
        return AdlsPath(container="transformed", blob=c.uri)

    @classmethod
    def curated(cls):
        c = cls()
        c.dir_template = ["product"]
        c.filename_template = None
        return AdlsPath(container="curated", blob=c.uri)

    @staticmethod
    def get_dataset_format(filename: str) -> DataLakeDatasetFileTypes:
        """Extracts dataset format from a path."""
        if filename.endswith(".parquet"):
            return "parquet"
        if filename.endswith(".csv"):
            return "csv"
        if filename.endswith(".json"):
            return "json"
        raise Exception(f"File Type of file `{filename}` not supported.")


PathInput: TypeAlias = str | Dict | AdlsPath | LocalPath | Path | XComGetter | PathlibPath
