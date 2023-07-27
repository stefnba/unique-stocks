from shared.utils.logging.types import LogEvent
from typing import Optional


class BaseRequest(LogEvent):
    job: Optional[str] = None
    dataset: Optional[str] = None
    product: Optional[str] = None


class ValidationViolation(BaseRequest):
    name: str = "ValidationViolation"
    violation: str
