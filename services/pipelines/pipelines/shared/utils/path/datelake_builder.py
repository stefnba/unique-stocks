from datetime import datetime
from string import Template

from pydantic import BaseModel
from shared.config import config
from shared.utils.path.types import DatalakeDate, DatalakeDirParams, DatalakeFileTypes

DataLakeZones = config.datalake.zones


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
        from shared.utils.path.builder import FilePathBuilder  # avoid circular import

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


class DatalakePath(
    DatalakePathBuilder,
    BaseModel,
):
    """
    Base class for creating complete paths in the datalake.

    The following properties need to be specified:
    - directory
    - file_name
    - file_type

    All others are optional and only need to be specified if they are list in child class.


    """

    zone: DataLakeZones  # type: ignore

    def _get_args(self) -> dict:
        """
        Returns all args specified for pydantic BaseModel.
        method.

        Returns:
            _type_: _description_
        """
        return self.dict()

    class Config:
        extra = "allow"
        # exclude = {"_args", "_template_path"}
