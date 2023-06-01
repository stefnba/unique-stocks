from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Mapping(BaseModel):
    # id: Optional[int]
    source: str
    product: str
    field: Optional[str] = None
    source_value: str
    source_description: Optional[str] = None
    uid: str
    uid_description: Optional[str] = None
    is_seed: Optional[bool] = False
    is_active: Optional[bool] = True


class MappingUid(BaseModel):
    source: str
    product: str
    field: str
    source_value: str
    uid: str
    uid_description: str
    is_seed: bool
    is_active: bool
    created_at: datetime
    updated_at: datetime
    active_from: datetime
    active_until: datetime
