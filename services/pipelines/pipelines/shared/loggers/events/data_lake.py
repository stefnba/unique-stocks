from shared.utils.logging.types import LogEvent


class BaseLogEvent(LogEvent):
    path: str
    format: str


class DownloadInit(BaseLogEvent):
    name: str = "DownloadInit"


class DownloadSuccess(BaseLogEvent):
    name: str = "DownloadSuccess"
    path: str
    format: str
    handler: str


class DownloadError(BaseLogEvent):
    name: str = "DownloadError"
    error: str
    handler: str


class UploadInit(LogEvent):
    name: str = "UploadInit"


class UploadSuccess(LogEvent):
    name: str = "UploadSuccess"


class UploadError(LogEvent):
    name: str = "UploadError"
