from datetime import datetime
from string import Template
from typing import TYPE_CHECKING, Optional, Type

from shared.config import config
from shared.utils.path.builder import FilePathBuilder
from shared.utils.path.datalake.types import DatalakeDate, DatalakeDirParams, DatalakeFileTypes
from shared.utils.path.types import PathParams

if TYPE_CHECKING:
    from shared.utils.path.datalake.path import DatalakePath

DatalakeZones = config.datalake.zones


class DatalakePathBuilder:
    directory: DatalakeDirParams
    file_name: str
    file_type: DatalakeFileTypes

    _full_path: str
    _template_path: str
    _args: dict

    __file_type_default: DatalakeFileTypes = "parquet"

    def __repr__(self) -> str:
        return self._build_full_path()

    def __str__(self) -> str:
        return self._build_full_path()

    def _get_args(self) -> dict:
        """
        Placeholder method.
        """
        return {}

    def _build_full_path(self) -> str:
        """
        Constructs full path consisting of dir, file name and extension.

        Method is called by __str__ and __repr__, so no need to call this method manually,
        since representation of class is already full path.

        Returns:
            str: Absolute file Path.
        """

        file_type = self.file_type if hasattr(self, "file_type") else self.__file_type_default

        _template_path = FilePathBuilder.build_file_path(
            file_name=self.file_name, file_path=self.directory, file_type=self.file_type, make_absolute=True
        )

        args = self._get_args()
        date_args = self.__get_date_args()
        combined_args = {**args, **{"file_type": file_type}, **date_args.dict()}

        try:
            _full_path = Template(_template_path).substitute(combined_args)
            return _full_path
        except KeyError as e:
            possible_keys = [f'"{k}"' for k in combined_args.keys()]
            key = str(e).replace("'", "")

            raise KeyError(f"Key \"{key}\" not found. Possible keys are: {', '.join(possible_keys)}") from None

    @staticmethod
    def part_cmpnt(path_component: str) -> str:
        """
        Creates a partitioned path component, usually for a directory, that follows Hive flavored partitioning pattern, i.e.
        /year=2022/month=12/day=30/...

        Args:
            path_component (str): Component that will be converted into key=${key} for Template string

        Returns:
            str: key=${key} string
        """

        return f"{path_component}=${{{path_component}}}"

    @staticmethod
    def __get_date_args() -> DatalakeDate:
        now = datetime.utcnow()

        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        minute = now.strftime("%M")
        second = now.strftime("%S")

        return DatalakeDate(year=year, day=day, month=month, minute=minute, hour=hour, second=second)

    @classmethod
    def build_abfs_path(
        cls, path: PathParams | "DatalakePath" | Type["DatalakePath"], file_system: Optional[str] = None
    ):
        """
        Constructs a abfs file system path for Azure DataLake Gen 2 by prepending `abfs://{file_system}/` to the
        specified path.

        Args:
            path: Path on file system.
            file_system: Defaults to `config.azure.file_system`.


        Returns:
            str: abfs:// file system path.
        """
        _file_system = file_system or config.azure.file_system
        _path = FilePathBuilder.convert_to_file_path(path)

        if not _file_system:
            raise Exception("No file system specified.")

        return f"abfs://{_file_system}/{_path.lstrip('/')}"
