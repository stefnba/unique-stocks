from typing import Literal, Optional, TypeVar

from pydantic import BaseModel
from shared.utils.path.types import PathParams

EndpointParam = PathParams

JsonResponse = TypeVar("JsonResponse", dict, list[dict])


class RequestFileReturn(BaseModel):
    extension: str
    name: str


class RequestFileBytesReturn(RequestFileReturn):
    content: bytes


class RequestFileDiskReturn(RequestFileReturn):
    path: str


Methods = Literal["GET", "POST", "PUT"]


class RequestParams(BaseModel):
    endpoint: str
    method: Methods = "GET"
    params: Optional[dict] = None
