from dataclasses import dataclass, field
from typing import Callable, Optional, Type

from shared.utils.path import path_builder
from shared.utils.path.path_builder import path_with_dateime
from shared.utils.path.types import DatalakeFileTypes, DatalakeStages, DirectoryObject, DirectorySetParams


class DatalakePathHelpers:
    path_with_dateime: Callable = path_with_dateime


class DatalakePathBuilderBase:
    # directory: str | list[str | None]
    directory: DirectorySetParams
    file_type: Optional[DatalakeFileTypes] = None
    file_name: str | list[str | None]

    helpers = DatalakePathHelpers

    __file_type_default: DatalakeFileTypes = "parquet"

    def __repr__(self):
        return f'DatalakePath(path="{self.build_final_path()})"'

    def get_dir_as_list(self) -> list[str]:
        """
        Returns directory path as list with each folder as an element in the list.
        """
        return self._build_dir(self.directory)

    def insert_into_dir_path(self, obj: DirectoryObject | str, index: int):
        """
        Inserts a new object into pre-defined directory path

        Args:
            object (DirectoryObject): object to add
            index (int): Positions at which to insert new object
        """
        _new = self._build_dir(obj)
        _existing_path = self.get_dir_as_list()
        _existing_path.insert(index, _new[0])

        self.directory = _existing_path

    def set_path(self):
        """
        Placeholder method for child classes that is used to specify
        - self.directory
        - self.file_name
        - self.file_type
        """

    def set_directory(self):
        """
        Placeholder method for child classes that is used to specify
        - self.directory
        """

    def set_file_type(self):
        """
        Placeholder method for child classes that is used to specify
        - self.file_name
        """

    def set_file_name(self):
        """
        Placeholder method for child classes that is used to specify
        - self.file_type
        """

    def _build_dir(self, dir_path: DirectorySetParams) -> list[str]:
        """
        Helper method to build directory path from a combination of str, dict, and list.

        Args:
            dir_path (DirectorySetParams): _description_

        Raises:
            Exception: _description_

        Returns:
            list[str | None] | str: _description_
        """
        if isinstance(dir_path, list):
            directory: list[str] = []
            for dir_item in dir_path:
                if isinstance(dir_item, str):
                    directory.append(dir_item)
                if isinstance(dir_item, dict):
                    directory.extend([f"{k}={v}" for k, v in dir_item.items() if v is not None])
            return directory

        if isinstance(dir_path, dict):
            return [f"{k}={v}" for k, v in dir_path.items() if v is not None]

        if isinstance(dir_path, str):
            return dir_path.split("/")

        raise Exception("Directory path must be specified as object.")

    def build_final_path(self) -> str:
        """
        Builds complete path
        """
        extension = self.file_type if hasattr(self, "file_type") else None

        # initialize path, directory & file_name & file_type
        self.set_path()

        # override either with separate method while keeping others as is
        self.set_directory()
        self.set_file_name()
        self.set_file_type()

        directory = self._build_dir(self.directory)

        final_path = path_builder.file_path(
            directory=directory,
            filename=self.file_name,
            extension=extension or self.file_type or self.__file_type_default,
        )
        return final_path

    @staticmethod
    def path_to_string(
        input_path: str | list[str | None] | Type["DatalakePathBuilderBase"] | "DatalakePathBuilderBase",
    ) -> str:
        """
        Converts input_path which can either be a string, DataLakeLocation class
        or instance of DataLakeLocation into a path string

        Args:
            input_path (str | Type[DataLakeLocation] | DataLakeLocation): input

        Returns:
            str: absolute path
        """
        # string
        if isinstance(input_path, str):
            return path_builder.path(input_path)
        # list
        if isinstance(input_path, list):
            return path_builder.path(input_path)
        # class
        if not isinstance(input_path, DatalakePathBuilderBase) and issubclass(input_path, DatalakePathBuilderBase):
            return path_builder.path(input_path().build_final_path())
        # class instantiated
        return path_builder.path(input_path.build_final_path())


@dataclass
class DatalakePathConfig(DatalakePathBuilderBase):
    """
    Class to specify path generic logic in datalake with build_path() method.
    Other location builders should inerhit from this class, not the base class
    DatalakePathBuilderBase.
    """

    stage: DatalakeStages
    asset_source: str
    product: str = field(init=False)
    asset: str = field(init=False)
    file_type: Optional[DatalakeFileTypes] = field(init=False)

    def set_path(self):
        self.directory = [
            self.stage,
            {
                "product": self.product,
                "asset": self.asset,
                "source": self.asset_source,
                "y": self.helpers.path_with_dateime("%Y"),
                "m": self.helpers.path_with_dateime("%m"),
            },
        ]
        self.file_name = [
            self.helpers.path_with_dateime("%Y%m%d"),
            self.asset_source,
            self.asset,
            self.stage,
        ]
        self.file_type = "parquet"
