from shared.utils.logging.types import LogEvent


class BaseRequest(LogEvent):
    sizeBytes: int
    sizeMegaBytes: float


class StreamInit(BaseRequest):
    name: str = "FileStreamInit"


class StreamSuccess(BaseRequest):
    name: str = "FileStreamSuccess"


class StreamStart(BaseRequest):
    name: str = "FileStreamStart"
