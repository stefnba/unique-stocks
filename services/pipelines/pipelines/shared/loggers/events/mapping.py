from shared.utils.logging.types import LogEvent


Test = LogEvent(name="testEvent1111")


class CustomEvent(LogEvent):
    name: str = "ISAJDL"
    hallo: str
