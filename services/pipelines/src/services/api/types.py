from pydantic import BaseModel
from typing import Literal, Optional, TypedDict

class RequestFileReturnBase(BaseModel):
    extension: str
    name: str

class RequestFileBytes(RequestFileReturnBase):
    content: bytes

class RequestFileDisk(RequestFileReturnBase):
    path: str


Methods = Literal["GET", "POST", "PUT"]
class RequestParams(BaseModel):
    endpoint: str
    method: Methods = "GET"
    params: Optional[dict] = None
