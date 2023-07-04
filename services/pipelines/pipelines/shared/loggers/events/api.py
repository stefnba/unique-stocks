from shared.utils.logging.types import LogEvent


class BaseRequest(LogEvent):
    url: str
    method: str


class RequestInit(BaseRequest):
    name: str = "RequestInit"


class RequestSuccess(BaseRequest):
    name: str = "RequestSuccess"


class RequestError(BaseRequest):
    name: str = "RequestError"
    error: str


class RequestTimeout(BaseRequest):
    name: str = "RequestTimeout"
