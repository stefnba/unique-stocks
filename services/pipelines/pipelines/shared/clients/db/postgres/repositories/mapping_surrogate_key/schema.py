from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class MappingSurrogateKey(BaseModel):
    surrogate_key: int
    product: str
    uid: str
    is_active: bool
    created_at: datetime
    updated_at: datetime
    valid_from: datetime
    valid_until: datetime
