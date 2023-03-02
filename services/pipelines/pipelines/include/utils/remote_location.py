from datetime import datetime
from typing import Literal, Type

from include.utils.path import build_file_path, build_path

FileExtTypes = Literal["csv", "json", "parquet"]


class DataLakeLocation:
    directory: str | list[str | None]
    file_extension: FileExtTypes
    file_name: str | list[str | None]

    def datetime_path(self, date_format: str = "%Y/%m/%d") -> str:
        """
        Helper function to build a path with current datetime.

        - Year yyyy: %Y
        - Month mm: %m
        - Day dd: %d

        Args:
            format (str, optional): Format how the data should be returned.
            Defaults to '%Y/%m/%d'.

        Returns:
            str: Current datetime in specified format as string
        """
        now = datetime.now()
        return now.strftime(date_format)

    def build_location(self):
        """
        Placeholder method that is used to define
        - self.directory
        - self.file_name
        - self.file_extension
        """
        return ""

    def build_final_path(self) -> str:
        """
        Builds complete path
        """
        extension = self.file_extension if hasattr(self, "file_extension") else None
        self.build_location()
        final_path = build_file_path(
            directory=self.directory,
            filename=self.file_name,
            extension=extension or self.file_extension,
        )
        return final_path


def translate_data_lake_location_into_string(
    input_path: str | list[str | None] | Type[DataLakeLocation] | DataLakeLocation,
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
        return build_path(input_path)
    # list
    if isinstance(input_path, list):
        return build_path(input_path)
    # class
    if not isinstance(input_path, DataLakeLocation) and issubclass(
        input_path, DataLakeLocation
    ):
        return build_path(input_path().build_final_path())
    # class instantiated
    return build_path(input_path.build_final_path())
