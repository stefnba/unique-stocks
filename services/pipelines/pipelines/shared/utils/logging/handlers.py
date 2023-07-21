import logging
from typing import Optional
from shared.utils.logging.formatter import BaseFormatter, TextFormatter, JsonFormatter
from pathlib import Path
import requests
from shared.config import CONFIG
from logging import LogRecord


class CustomHttp(logging.Handler):
    url: str
    session: requests.Session

    def __init__(
        self,
    ) -> None:
        # self.host = CONFIG.logging.host
        # self.endpoint = CONFIG.logging.endpoint
        # self.port = CONFIG.logging.port

        self.url = f"{CONFIG.logging.host}:{CONFIG.logging.port}/{CONFIG.logging.endpoint.lstrip('/')}"
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
        # add service
        record.service = "PIPELINES"

        json_record = self.format(record)

        try:
            self.session.post(self.url, data=json_record, timeout=0.000000001)
        except requests.Timeout:
            pass
        except Exception as error:
            print("Log HTTP handler not working", json_record, self.url, error)


class BaseHandler:
    formatter: BaseFormatter = TextFormatter()
    handler: logging.Handler

    def __init__(self, formatter: Optional[BaseFormatter] = None) -> None:
        if formatter:
            self.formatter = formatter


class HttpHandler(BaseHandler):
    formatter = JsonFormatter()
    handler = CustomHttp()


class ConsoleHandler(BaseHandler):
    handler = logging.StreamHandler()
    formatter = TextFormatter()


class FileHandler(BaseHandler):
    handler: logging.FileHandler
    loggername_as_filename = False  # log filename based on logger name if True
    path: str
    filename: str

    def __init__(
        self, filename: Optional[str] = None, path: str = "./", formatter: Optional[BaseFormatter] = None
    ) -> None:
        super().__init__(formatter=formatter)

        self.path = path

        if filename:
            self.filename = filename
            self.assign_handler()
        else:
            self.loggername_as_filename = True

    def build_file_path(self):
        ext = "log"

        Path(self.path).mkdir(parents=True, exist_ok=True)

        return f"{self.path}/{self.filename.lower()}.{ext}"

    def assign_handler(self):
        location = self.build_file_path()
        self.handler = logging.FileHandler(filename=location)
