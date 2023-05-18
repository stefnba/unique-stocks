import uuid
from datetime import datetime
from string import Template
from typing import Any, Dict, Optional, Type

from pydantic import BaseModel
from shared.config import CONFIG
from shared.utils.path.builder import FilePathBuilder
from shared.utils.path.data_lake.types import DatalakeDate, DatalakeFileTypes, DataLakePathPattern, DataLakePathVersions
from shared.utils.path.types import PathParams

DataLakeZones = CONFIG.data_lake.zones


class PathPatterns:
    """
    Collection of pattern for directory and file names.
    """

    date_dir = ["year=${year}", "month=${month}", "day=${day}"]
    date_file = "${year}${month}${day}"
    datetime_file = "${year}${month}${day}_${hour}${minute}${second}__"


class DataLakeFilePathModel(BaseModel):
    """All DataLakePath models should inherit from this one. Alias for pydantic.BaseModel"""

    dir_pattern: DataLakePathPattern
    file_name_pattern: DataLakePathPattern
    file_type: DatalakeFileTypes


class DataLakeFilePath:
    """Constructs file path for storing and retrieving files in the Data Lake"""

    class Properties:
        """Specifies which properties are required to construct DataLakePath using DataLakeFilePathModel"""

        class Base(DataLakeFilePathModel):
            zone: DataLakeZones  # type: ignore # valid-type
            product: str
            dir_pattern: DataLakePathPattern = [
                "zone=${zone}",
                "product=${product}",
                "source=${source}",
                *PathPatterns.date_dir,
            ]
            file_name_pattern: DataLakePathPattern = [
                f"ts={PathPatterns.datetime_file}",
                "product=${product}",
                "source=${source}",
                "zone=${zone}",
            ]

            # for bin path
            bin_name: Optional[str] = None
            bin: Optional[str] = None

        class Curated(Base):
            file_type: DatalakeFileTypes = "parquet"  # .parquet as default
            zone: DataLakeZones = "curated"  # type: ignore # valid-type
            version: DataLakePathVersions = "current"
            dir_pattern: DataLakePathPattern = ["zone=${zone}", "product=${product}", "version=${version}"]
            file_name_pattern: DataLakePathPattern = "${product}__${version}"

        class CuratedHistory(Base):
            file_type: DatalakeFileTypes = "parquet"  # .parquet as default
            zone: DataLakeZones = "curated"  # type: ignore # valid-type
            version: DataLakePathVersions = "history"
            dir_pattern: DataLakePathPattern = [
                "zone=${zone}",
                "product=${product}",
                "version=${version}",
                *PathPatterns.date_dir,
            ]
            file_name_pattern: DataLakePathPattern = PathPatterns.datetime_file + "${product}__${version}"

        class Raw(Base):
            source: str
            zone: DataLakeZones = "raw"  # type: ignore # valid-type

        class Modeled(Base):
            source: str
            zone: DataLakeZones = "modeled"  # type: ignore # valid-type

        class Processed(Base):
            file_type: DatalakeFileTypes = "parquet"  # .parquet as default
            source: str
            zone: DataLakeZones = "processed"  # type: ignore # valid-type

        class Temp(DataLakeFilePathModel):
            key = uuid.uuid4().hex
            zone: DataLakeZones = "temp"  # type: ignore # valid-type
            dir_pattern: DataLakePathPattern = ["zone=${zone}"]
            file_name_pattern: DataLakePathPattern = PathPatterns.datetime_file + "${key}"
            file_type: DatalakeFileTypes = "parquet"

    class Generics:
        """
        Exposes generic methods that can be used in child Path classes. Any method must return an instance
        of DataLakeFilePathModel.
        """

        class Default:
            product: str

            @classmethod
            def raw(cls, source: str, file_type: DatalakeFileTypes):
                """File Path for Raw zone."""
                return DataLakeFilePath.Properties.Raw(source=source, file_type=file_type, product=cls.product)

            @classmethod
            def processed(cls, source: str):
                """File Path for Processed zone."""
                return DataLakeFilePath.Properties.Processed(source=source, product=cls.product)

            @classmethod
            def temp(cls):
                """File Path for Temp zone."""
                return DataLakeFilePath.Properties.Temp()

            @classmethod
            def curated(cls, version: DataLakePathVersions = "current"):
                """File Path for Curated zone."""

                if version == "history":
                    return DataLakeFilePath.Properties.CuratedHistory(product=cls.product)

                return DataLakeFilePath.Properties.Curated(product=cls.product)

        class Bin:
            """Generics for assets that consist of various bins, e.g. exchange securities, index constituents"""

            bin_name: str
            product: str
            dir_pattern: DataLakePathPattern = [
                "zone=${zone}",
                "product=${product}",
                "${bin_name}=${bin}",
                "source=${source}",
                *PathPatterns.date_dir,
            ]
            file_name_pattern: DataLakePathPattern = (
                PathPatterns.datetime_file + "${source}__${product}__${bin}__${zone}"
            )

            @classmethod
            def raw(cls, source: str, file_type: DatalakeFileTypes, bin: str):
                """File Path for Raw zone with bins."""
                return DataLakeFilePath.Properties.Raw(
                    source=source,
                    file_type=file_type,
                    product=cls.product,
                    bin_name=cls.bin_name,
                    bin=bin,
                    file_name_pattern=cls.file_name_pattern,
                    dir_pattern=cls.dir_pattern,
                )

            @classmethod
            def processed(cls, source: str, bin: str):
                """File Path for Processed zone with bins."""
                return DataLakeFilePath.Properties.Processed(
                    source=source,
                    product=cls.product,
                    bin_name=cls.bin_name,
                    bin=bin,
                    file_name_pattern=cls.file_name_pattern,
                    dir_pattern=cls.dir_pattern,
                )

            @classmethod
            def curated(cls, bin: str, version: DataLakePathVersions = "current"):
                """File Path for Curated zone with bins."""

                if version == "history":
                    return DataLakeFilePath.Properties.CuratedHistory(product=cls.product, bin=bin)

                dir_pattern: DataLakePathPattern = [
                    "zone=${zone}",
                    "product=${product}",
                    "version=${version}",
                    "${bin_name}=${bin}",
                ]

                return DataLakeFilePath.Properties.Curated(
                    product=cls.product, bin=bin, dir_pattern=dir_pattern, bin_name=cls.bin_name
                )

            @classmethod
            def temp(cls):
                """File Path for Temp zone."""
                return DataLakeFilePath.Properties.Temp()

    class Builder:
        """Builds a file system path from DataLakeFilePath class"""

        file_type: DatalakeFileTypes
        file_name_pattern: DataLakePathPattern
        dir_pattern: DataLakePathPattern
        template_args: Dict[str, Any]

        def __init__(self, file_path: DataLakeFilePathModel) -> None:
            if not isinstance(file_path, DataLakeFilePathModel):
                raise Exception("file_path is not an instance of DataLakeFilePathModel")

            self.file_type = file_path.file_type
            self.file_name_pattern = file_path.file_name_pattern
            self.dir_pattern = file_path.dir_pattern

            self.template_args = file_path.dict()

        def __repr__(self) -> str:
            return self._build_full_path()

        def __str__(self) -> str:
            return self._build_full_path()

        def _build_full_path(self) -> str:
            """
            Constructs full path consisting of dir, file name and extension.

            Method is called by __str__ and __repr__, so no need to call this method manually,
            since representation of class is already full path.

            Returns:
                str: Absolute file Path.
            """

            _template_path = FilePathBuilder.build_file_path(
                file_name=self.file_name_pattern,
                file_path=self.dir_pattern,
                file_type=self.file_type,
                make_absolute=True,
            )

            date_args = self.__get_date_args()
            combined_args = {**self.template_args, **{"file_type": self.file_type}, **date_args.dict()}

            try:
                _full_path = Template(_template_path).substitute(combined_args)
                return _full_path
            except KeyError as e:
                possible_keys = [f'"{k}"' for k in combined_args.keys()]
                key = str(e).replace("'", "")

                raise KeyError(f"Key \"{key}\" not found. Possible keys are: {', '.join(possible_keys)}") from None

        @staticmethod
        def __get_date_args() -> DatalakeDate:
            """
            Get current datetime for DataLakeFilePath construction.
            """
            now = datetime.utcnow()

            year = now.strftime("%Y")
            month = now.strftime("%m")
            day = now.strftime("%d")
            hour = now.strftime("%H")
            minute = now.strftime("%M")
            second = now.strftime("%S")

            return DatalakeDate(year=year, day=day, month=month, minute=minute, hour=hour, second=second)

    @staticmethod
    def build_abfs_path(path: PathParams | "DataLakeFilePathModel", file_system: Optional[str] = None):
        """
        Constructs a abfs file system path for Azure DataLake Gen 2 by prepending `abfs://{file_system}/` to the
        specified path.

        Args:
            path: Path on file system.
            file_system: Defaults to `configazure.file_system`.


        Returns:
            str: abfs:// file system path.
        """
        _file_system = file_system or CONFIG.azure.file_system
        _path = FilePathBuilder.convert_to_file_path(path)

        if not _file_system:
            raise Exception("No file system specified.")

        return f"abfs://{_file_system}/{_path.lstrip('/')}"
