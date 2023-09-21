from shared.utils.logging.types import LogEvent


class BaseEvent(LogEvent):
    job: str


class MissingValue(BaseEvent):
    name: str = "MissingValue"
    data: object | list[object]


class Sucess(BaseEvent):
    name: str = "TransformSuccess"


class MultipleRecords(LogEvent):
    name: str = "MultipleRecords"
