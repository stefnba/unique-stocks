from datetime import datetime
from typing import Optional

from pydantic import BaseModel


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
    valid_from: datetime
    valid_until: datetime
