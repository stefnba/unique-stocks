from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Exchange(BaseModel):
    id: int
    operating_exchange_id: Optional[int]
    name: str
    country_id: Optional[str]
    currency: Optional[str]
    website: Optional[str]
    timezone: Optional[str]
    comment: Optional[str]
    source: Optional[str]
    is_virtual: Optional[bool]
    acronym: Optional[str]
    mic: Optional[str]
    status: Optional[str]
    # operating_mic: Optional[str]
    # created_at: Optional[datetime] = datetime.now()
    updated_at: Optional[datetime]
