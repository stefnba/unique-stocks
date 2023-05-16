from typing import Literal, Optional

Levels = Literal["INFO", "ERROR", "CRITICAL", "WARNING", "DEBUG"]
FileHandlerFormats = Literal[
    "JSON",
    "TEXT",
]
Extra = Optional[object | list]
