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
    acronym: Optional[str]
    mic: Optional[str]
    # operating_mic: Optional[str]
    source: Optional[str]
    # created_at: Optional[datetime] = datetime.now()
    updated_at: Optional[datetime]
