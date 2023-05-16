import logging
from typing import Optional

from shared.utils.logging.formatter import BaseFormatter, JSONFormatter
from shared.utils.logging.types import FileHandlerFormats

ROOT_FORMAT = (
    "%(asctime)s-%(levelname)s-%(extra)s-%(name)s::[%(filename)s:%(lineno)d]::%(module)s|%(lineno)s:: %(message)s"
)


class BaseHandler:
    format = BaseFormatter(None)

    def __init__(self, format: FileHandlerFormats = "TEXT") -> None:
        if format == "JSON":
            self.format = JSONFormatter(None)

    def get_handler(self) -> logging.Handler:
        """
        Placeholder method to return handler. Will to overwritten by actual handlers.
        """
        return logging.Handler()


class Handlers:
    class Console(BaseHandler):
        """
        Logs to console.
        """

        def __init__(self, format: FileHandlerFormats = "TEXT") -> None:
            super().__init__(format)

        def get_handler(self):
            """
            Returns handler.
            """
            return logging.StreamHandler()

    class File(BaseHandler):
        """
        Logs to file.
        """

        file_path: str
        file_name: Optional[str]

        def __init__(
            self,
            file_name: Optional[str] = None,
            *,
            file_path: str,
            format: FileHandlerFormats = "TEXT",
        ) -> None:
            super().__init__(format)

            self.file_path = file_path
            self.file_name = file_name

        def get_handler(self):
            """
            Returns handler.
            """
            file_name = self.build_file_name()
            return logging.FileHandler(filename=file_name, mode="a", encoding="utf-8")

        def build_file_name(self) -> str:
            """
            Constructs file_name based on name and path.

            Args:
                name (str):
                path (str):

            Returns:
                str: constructed full file_name.
            """
            ext = "log"
            return f"{self.file_path}/{self.file_name or 'out'}.{ext}"
