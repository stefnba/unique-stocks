from shared.utils.logging.types import LogEvent


class BaseRequest(LogEvent):
    job: str


class Init(BaseRequest):
    name: str = "JobInit"


class Success(BaseRequest):
    name: str = "JobSuccess"


class Error(BaseRequest):
    name: str = "JobError"
    error: str
