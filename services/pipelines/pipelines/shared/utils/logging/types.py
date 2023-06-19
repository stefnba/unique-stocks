from pydantic import BaseModel


class LogEvent(BaseModel):
    name: str

    def __name__(self) -> str:
        return str(self.__name__)
