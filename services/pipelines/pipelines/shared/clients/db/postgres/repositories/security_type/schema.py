from typing import Optional

from pydantic import BaseModel


class SecurityType(BaseModel):
    id: int
    parent_id: Optional[int]
    name: str
    name_figi: Optional[str]
    name_figi2: Optional[str]
    market_sector_figi: Optional[str]
