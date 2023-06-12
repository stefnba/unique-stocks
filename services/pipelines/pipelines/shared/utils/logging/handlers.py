import logging
from typing import Optional, cast
import requests
import json

# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry
from logging import LogRecord, handlers
from shared.utils.logging.formatter import BaseFormatter, JSONFormatter
from shared.utils.logging.types import FileHandlerFormats
from shared.config import CONFIG

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


class CustomHttp(logging.Handler):
    # host: str
    # port: int
    # endpoint: str
    url: str
    session: requests.Session

    def __init__(
        self,
    ) -> None:
        # self.host = CONFIG.logging.host
        # self.endpoint = CONFIG.logging.endpoint
        # self.port = CONFIG.logging.port

        self.url = f"{CONFIG.logging.host}:{CONFIG.logging.port}/{ CONFIG.logging.endpoint.lstrip('/')}"
        print(self.url)
        # sets up a session with the server
        # MAX_POOLSIZE = 100

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                # 'Authorization': 'Bearer %s' % (self.token)
            }
        )

        super().__init__()

    def emit(self, record: LogRecord) -> None:
        # convert json string to dict
        record_dict = json.loads(self.format(record))

        extra = record_dict.pop("extra")

        # add service property for remote service
        record_dict["service"] = "pipeline"

        # post log to remote service
        self.session.post(self.url, data=json.dumps({**record_dict, **extra}))


class CustomHttpHandler(handlers.HTTPHandler):
    def __init__(self) -> None:
        host = CONFIG.logging.host
        endpoint = CONFIG.logging.endpoint
        port = CONFIG.logging.port
        method = "POST"

        super().__init__(host=f"{host}:{port}", url=endpoint, method=method)

    def mapLogRecord(self, record: logging.LogRecord):
        return {"service": "Pipeline", "test": 123}


class Handlers:
    class Http(BaseHandler):
        format = JSONFormatter(None)
        # def __init__(self) -> None:
        #     super().__init__(format="JSON")

        def get_handler(self):
            return CustomHttp()

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
